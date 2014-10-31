/**
 * 
 */
package edu.ucsd.forward.query.logical.visitors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import edu.ucsd.app2you.util.config.Config;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.DataSourceMetaData.StorageSystem;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.query.ast.SetOpExpression.SetOpType;
import edu.ucsd.forward.query.ast.SetQuantifier;
import edu.ucsd.forward.query.function.FunctionCall;
import edu.ucsd.forward.query.function.external.ExternalFunctionCall;
import edu.ucsd.forward.query.logical.AntiSemiJoin;
import edu.ucsd.forward.query.logical.ApplyPlan;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.logical.Copy;
import edu.ucsd.forward.query.logical.EliminateDuplicates;
import edu.ucsd.forward.query.logical.Exists;
import edu.ucsd.forward.query.logical.Ground;
import edu.ucsd.forward.query.logical.GroupBy;
import edu.ucsd.forward.query.logical.GroupBy.Aggregate;
import edu.ucsd.forward.query.logical.IndexScan;
import edu.ucsd.forward.query.logical.InnerJoin;
import edu.ucsd.forward.query.logical.Navigate;
import edu.ucsd.forward.query.logical.OffsetFetch;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.OuterJoin;
import edu.ucsd.forward.query.logical.PartitionBy;
import edu.ucsd.forward.query.logical.Product;
import edu.ucsd.forward.query.logical.Project;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.Select;
import edu.ucsd.forward.query.logical.SemiJoin;
import edu.ucsd.forward.query.logical.SendPlan;
import edu.ucsd.forward.query.logical.SetOperator;
import edu.ucsd.forward.query.logical.Sort;
import edu.ucsd.forward.query.logical.Subquery;
import edu.ucsd.forward.query.logical.ddl.CreateDataObject;
import edu.ucsd.forward.query.logical.ddl.DropDataObject;
import edu.ucsd.forward.query.logical.dml.Delete;
import edu.ucsd.forward.query.logical.dml.Insert;
import edu.ucsd.forward.query.logical.dml.Update;
import edu.ucsd.forward.query.logical.dml.Update.Assignment;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.plan.LogicalPlanUtil;
import edu.ucsd.forward.query.logical.term.AbsoluteVariable;
import edu.ucsd.forward.query.logical.term.Constant;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.Parameter.InstantiationMethod;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;

/**
 * The operator visitor that determines the execution data source of all non-distributed operators.
 * 
 * @author <Vicky Papavasileiou>
 * @author Michalis Petropoulos
 * 
 */
public final class LogicalPlanDistributorDefault extends AbstractOperatorVisitor
{
    /**
     * Stores for each variable to which data source it corresponds.
     */
    private Map<Term, String>                   m_vars_to_sources;
    
    private UnifiedApplicationState             m_uas;
    
    private Stack<LogicalPlan>                  m_enclosing_plan;
    
    /**
     * Children of set operators have overlapping variables with possibly different sources. This map is used to "save" the map
     * m_vars_to_sources when a set operator is encountered.
     */
    private Map<SetOperator, Map<Term, String>> m_setop_to_vars_to_sources;
    
    /**
     * Hidden constructor.
     * 
     * @param uas
     *            the unified application state.
     */
    private LogicalPlanDistributorDefault(UnifiedApplicationState uas)
    {
        assert (uas != null);
        m_uas = uas;
        m_enclosing_plan = new Stack<LogicalPlan>();
        m_vars_to_sources = new HashMap<Term, String>();
        m_setop_to_vars_to_sources = new HashMap<SetOperator, Map<Term, String>>();
    }
    
    /**
     * Gets a new instance.
     * 
     * @param uas
     *            the unified application state.
     * @return a new instance.
     */
    public static LogicalPlanDistributorDefault getInstance(UnifiedApplicationState uas)
    {
        return new LogicalPlanDistributorDefault(uas);
    }
    
    /**
     * Visits the input logical plan, assigns execution data source to operators that are not distributed. First, decide sources for
     * Assign operators.
     * 
     * @param plan
     *            the logical plan to visit.
     */
    public void annotate(LogicalPlan plan)
    {
        assert (plan != null);
        
        m_enclosing_plan.push(plan);
        
        for (Assign assign : plan.getAssigns())
        {
            assign.accept(this);
        }
        
        Stack<Operator> stack = new Stack<Operator>();
        LogicalPlanUtil.stackBottomUpRightToLeft(plan.getRootOperator(), stack);
        
        while (!stack.isEmpty())
        {
            Operator next = stack.pop();
            next.accept(this);
            
            if (next.getParent() instanceof SetOperator)
            {
                SetOperator parent = (SetOperator) next.getParent();
                
                // Saves a copy of the current map of variables
                if (m_setop_to_vars_to_sources.get(parent) == null)
                {
                    Map<Term, String> vars_to_sources_copy = new HashMap<Term, String>(m_vars_to_sources);
                    m_setop_to_vars_to_sources.put(parent, vars_to_sources_copy);
                }
                
                // Clear the current map of variables to sources, because the children have overlapping ones.
                m_vars_to_sources.clear();
            }
        }
        if (!m_enclosing_plan.isEmpty()) m_enclosing_plan.pop();
    }
    
    /**
     * Visits the input logical plan, assigns execution data source to operators that are not distributed.
     * 
     * @param plan
     *            the logical plan to visit.
     * @param start
     *            the root operator to start from
     */
    public void annotate(LogicalPlan plan, Operator start)
    {
        assert (plan != null);
        assert (start != null);
        
        LinkedList<Operator> queue = new LinkedList<Operator>();
        LogicalPlanUtil.queueTopDownRightToLeft(start, queue);
        
        while (!queue.isEmpty())
        {
            Operator next = queue.get(0);
            queue.remove(0);
            next.accept(this);
            
            if (next.getParent() instanceof SetOperator)
            {
                SetOperator parent = (SetOperator) next.getParent();
                
                // Saves a copy of the current map of variables
                if (m_setop_to_vars_to_sources.get(parent) == null)
                {
                    Map<Term, String> vars_to_sources_copy = new HashMap<Term, String>(m_vars_to_sources);
                    m_setop_to_vars_to_sources.put(parent, vars_to_sources_copy);
                }
                
                // Clear the current map of variables to sources, because the children have overlapping ones.
                m_vars_to_sources.clear();
            }
        }
    }
    
    @Override
    public Operator visitAntiSemiJoin(AntiSemiJoin operator)
    {
        this.assignSource(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitApplyPlan(ApplyPlan operator)
    {
        // Distribute the apply plan
        LogicalPlanDistributorDefault distributor = LogicalPlanDistributorDefault.getInstance(m_uas);
        distributor.m_vars_to_sources = new HashMap<Term, String>(m_vars_to_sources);
        distributor.m_enclosing_plan.addAll(m_enclosing_plan);
        distributor.annotate(operator.getLogicalPlansUsed().get(0));
        
        LogicalPlanUtil.setExecutionDataSourceName(operator, DataSource.MEDIATOR, m_uas);
        m_vars_to_sources.put(new RelativeVariable(operator.getAlias()), DataSource.MEDIATOR);
        // this.distributeOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitCopy(Copy operator)
    {
        // Distribute the copy plan
        LogicalPlanDistributorDefault distributor = LogicalPlanDistributorDefault.getInstance(m_uas);
        distributor.m_enclosing_plan.addAll(this.m_enclosing_plan);
        distributor.annotate(operator.getCopyPlan());
        
        this.assignSource(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitEliminateDuplicates(EliminateDuplicates operator)
    {
        this.assignSource(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitExists(Exists operator)
    {
        // Distribute the exists plan
        LogicalPlanDistributorDefault distributor = LogicalPlanDistributorDefault.getInstance(m_uas);
        distributor.m_vars_to_sources = new HashMap<Term, String>(m_vars_to_sources);
        distributor.m_enclosing_plan.addAll(this.m_enclosing_plan);
        distributor.annotate(operator.getLogicalPlansUsed().get(0));
        
        // FIXME The presence of parameters in the nested plan make it hard to push this operator to a JDBC data source.
        // this.distributeOperator(operator);
        LogicalPlanUtil.setExecutionDataSourceName(operator, DataSource.MEDIATOR, m_uas);
        
        // Adds the new variable mapping
        addNewVariableMappings(operator, operator.getOutputInfo().getVariables());
        
        return operator;
    }
    
    @Override
    public Operator visitGround(Ground operator)
    {
        // Do nothing
        return operator;
    }
    
    @Override
    public Operator visitGroupBy(GroupBy operator)
    {
        for (Aggregate agg : operator.getAggregates())
        {
            if (!agg.getAggregateFunctionCall().getFunction().isSqlCompliant())
            {
                operator.setExecutionDataSourceName(DataSource.MEDIATOR);
                return operator;
            }
            
        }
        this.assignSource(operator);
        
        // Adds the new variable mapping
        addNewVariableMappings(operator, operator.getOutputInfo().getVariables());
        
        return operator;
    }
    
    @Override
    public Operator visitInnerJoin(InnerJoin operator)
    {
        if (LogicalPlanUtil.condition_is_Sql_Compliant(operator.getConditions()) == false)
        {
            LogicalPlanUtil.setExecutionDataSourceName(operator, DataSource.MEDIATOR, m_uas);
            return operator;
        }
        
        this.assignSource(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitNavigate(Navigate operator)
    {
        if (operator.getChild() instanceof Ground)
        {
            Set<? extends Term> vars = operator.getOutputInfo().getVariables();
            addNewVariableMappings(operator, vars);
            this.assignSource(operator);
        }
        else if (operator.getTerm().getType() instanceof CollectionType || operator.getTerm().getType() instanceof TupleType)
        {
            operator.setExecutionDataSourceName(DataSource.MEDIATOR);
            addNewVariableMappings(operator, operator.getOutputInfo().getVariables());
        }
        else
        {
            operator.setExecutionDataSourceName(operator.getChild().getExecutionDataSourceName());
            addNewVariableMappings(operator, operator.getOutputInfo().getVariables());
        }
        
        return operator;
    }
    
    @Override
    public Operator visitOffsetFetch(OffsetFetch operator)
    {
        this.assignSource(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitOuterJoin(OuterJoin operator)
    {
        this.assignSource(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitProduct(Product operator)
    {
        this.assignSource(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitProject(Project operator)
    {
        if (!(operator.getChild() instanceof Ground))
        {
            this.assignSource(operator);
            // addNewVariableMappings(operator, operator.getOutputInfo().getVariables());
        }
        else
        {
            Set<? extends Term> vars = operator.getOutputInfo().getVariables();
            addNewVariableMappings(operator, vars);
            this.assignSource(operator);
        }
        
        // When do we get here:
        // 1.If the Project contains only constant and the child is ground
        if (operator.getExecutionDataSourceName() == null) operator.setExecutionDataSourceName(DataSource.MEDIATOR);
        
        // When do we get here:
        // When project contains mediator only function
        try
        {
            if (!operator.isDataSourceCompliant(m_uas.getDataSource(operator.getExecutionDataSourceName()).getMetaData()))
            {
                operator.setExecutionDataSourceName(DataSource.MEDIATOR);
            }
            return operator;
        }
        catch (DataSourceException e)
        {
            throw new AssertionError(e);
        }
    }
    
    @Override
    public Operator visitScan(Scan operator)
    {
        if (operator.getTerm() instanceof Parameter) // The instantiation method is assign
        {
            assert (((Parameter) operator.getTerm()).getInstantiationMethod().equals(InstantiationMethod.ASSIGN));
            operator.setExecutionDataSourceName(DataSource.MEDIATOR);
            return operator;
        }
        
        if (operator.getTerm() instanceof AbsoluteVariable)
        {
            AbsoluteVariable scan_var = (AbsoluteVariable) operator.getTerm();
            if (scan_var.getDataSourceName().equals(DataSource.TEMP_ASSIGN_SOURCE))
            {
                Stack<LogicalPlan> temp_stack = new Stack<LogicalPlan>();
                temp_stack.addAll(m_enclosing_plan);
                
                while (!temp_stack.isEmpty())
                {
                    LogicalPlan plan = temp_stack.pop();
                    // Find the source of the Assign that has this variable
                    for (Assign a : plan.getAssigns())
                    {
                        if (a.getTarget().equals(scan_var.getDefaultProjectAlias()))
                        {
                            operator.setExecutionDataSourceName(a.getExecutionDataSourceName());
                            break;
                        }
                    }
                    if (operator.getExecutionDataSourceName() != null) break;
                }
                
                assert (operator.getExecutionDataSourceName() != null);
                
                addNewVariableMappings(operator, operator.getOutputInfo().getVariables());
                
                return operator;
            }
        }
        
        if (!(operator.getChild() instanceof Ground))
        {
            operator.setExecutionDataSourceName(DataSource.MEDIATOR);
        }
        
        addNewVariableMappings(operator, operator.getOutputInfo().getVariables());
        
        this.assignSource(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitSelect(Select operator)
    {
        if (LogicalPlanUtil.condition_is_Sql_Compliant(operator.getConditions()) == false)
        {
            LogicalPlanUtil.setExecutionDataSourceName(operator, DataSource.MEDIATOR, m_uas);
            return operator;
        }
        
        if (operator.getChild() instanceof Ground)
        {
            Set<? extends Term> vars = operator.getOutputInfo().getVariables();
            addNewVariableMappings(operator, vars);
            this.assignSource(operator);
        }
        else
        {
            operator.setExecutionDataSourceName(operator.getChild().getExecutionDataSourceName());
        }
        
        // When do we get here:
        // 1. If the Select condition contains only constant and the child is ground
        if (operator.getExecutionDataSourceName() == null) operator.setExecutionDataSourceName(DataSource.MEDIATOR);
        
        return operator;
    }
    
    @Override
    public Operator visitSemiJoin(SemiJoin operator)
    {
        this.assignSource(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitSendPlan(SendPlan operator)
    {
        // Distribute the send plan
        LogicalPlanDistributorDefault distributor = LogicalPlanDistributorDefault.getInstance(m_uas);
        distributor.m_enclosing_plan.addAll(this.m_enclosing_plan);
        distributor.annotate(operator.getSendPlan());
        
        return operator;
    }
    
    @Override
    public Operator visitSetOperator(SetOperator operator)
    {
        // Special case if we don't allow ship union
        if (operator.getSetOpType() == SetOpType.UNION && operator.getSetQuantifier() == SetQuantifier.DISTINCT
                && Config.getOptimizationsNoShipUnion())
        {
            String left_source = operator.getChildren().get(0).getExecutionDataSourceName();
            String right_source = operator.getChildren().get(1).getExecutionDataSourceName();
            
            if (left_source != null && right_source != null && !left_source.equals(right_source))
            {
                operator.setExecutionDataSourceName(DataSource.MEDIATOR);
            }
        }
        this.assignSource(operator);
        
        // Restore the m_vars_to_sources that was deleted to be able to translate the children
        Map<Term, String> saved_vars_to_sources = m_setop_to_vars_to_sources.get(operator);
        assert (saved_vars_to_sources != null);
        
        m_vars_to_sources = saved_vars_to_sources;
        
        return operator;
    }
    
    @Override
    public Operator visitSort(Sort operator)
    {
        List<Term> order_terms = new ArrayList<Term>();
        for (edu.ucsd.forward.query.logical.Sort.Item item : operator.getSortItems())
        {
            order_terms.add(item.getTerm());
        }
        
        if (LogicalPlanUtil.condition_is_Sql_Compliant(order_terms) == false)
        {
            operator.setExecutionDataSourceName(DataSource.MEDIATOR);
            return operator;
        }
        
        this.assignSource(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitCreateSchemaObject(CreateDataObject operator)
    {
        // Distribute the default plans
        for (SchemaPath path : operator.getTypesToSetDefault())
        {
            LogicalPlan plan = operator.getDefaultPlan(path);
            LogicalPlanDistributorDefault distributor = LogicalPlanDistributorDefault.getInstance(m_uas);
            distributor.annotate(plan);
        }
        operator.setExecutionDataSourceName(operator.getDataSourceName());
        return operator;
    }
    
    @Override
    public Operator visitDropSchemaObject(DropDataObject operator)
    {
        operator.setExecutionDataSourceName(operator.getDataSourceName());
        return operator;
    }
    
    @Override
    public Operator visitInsert(Insert operator)
    {
        // Distribute the insert plan
        LogicalPlanDistributorDefault distributor = LogicalPlanDistributorDefault.getInstance(m_uas);
        distributor.annotate(operator.getLogicalPlansUsed().get(0));
        
        // FIXME The presence of parameters in the nested plan make it hard to push this operator to a JDBC data source.
        // this.distributeOperator(operator);
        LogicalPlanUtil.setExecutionDataSourceName(operator, DataSource.MEDIATOR, m_uas);
        
        return operator;
    }
    
    @Override
    public Operator visitDelete(Delete operator)
    {
        // FIXME The presence of parameters in the nested plan make it hard to push this operator to a JDBC data source.
        // this.distributeOperator(operator);
        LogicalPlanUtil.setExecutionDataSourceName(operator, DataSource.MEDIATOR, m_uas);
        
        return operator;
    }
    
    @Override
    public Operator visitUpdate(Update operator)
    {
        // Distribute the assignment plans
        for (Assignment assignment : operator.getAssignments())
        {
            LogicalPlanDistributorDefault distributor = LogicalPlanDistributorDefault.getInstance(m_uas);
            distributor.m_enclosing_plan.addAll(this.m_enclosing_plan);
            distributor.annotate(assignment.getLogicalPlan());
        }
        
        // FIXME The presence of parameters in the nested plan make it hard to push this operator to a JDBC data source.
        // this.distributeOperator(operator);
        LogicalPlanUtil.setExecutionDataSourceName(operator, DataSource.MEDIATOR, m_uas);
        
        return operator;
    }
    
    @Override
    public Operator visitPartitionBy(PartitionBy operator)
    {
        this.assignSource(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitIndexScan(IndexScan operator)
    {
        Set<? extends Term> vars = operator.getOutputInfo().getVariables();
        addNewVariableMappings(operator, vars);
        this.assignSource(operator);
        
        return operator;
    }
    
    private void assignSource(Operator operator)
    {
        
        if (operator.getExecutionDataSourceName() != null) return;
        List<String> sources = new ArrayList<String>();
        
        if (operator.getChildren().size() == 1 && operator.getChildren().get(0) instanceof Ground)
        {
            for (Term out_var : operator.getOutputInfo().getVariables())
            {
                String src = m_vars_to_sources.get(out_var);
                if (src != null)
                {
                    sources.add(src);
                }
            }
        }
        else
        {
            for (Operator child : operator.getChildren())
            {
                if (child instanceof Ground) continue;
                sources.add(child.getExecutionDataSourceName());
                
                // HACK: check if the output has collection-type value
                for (RelativeVariable var : child.getOutputInfo().getVariables())
                {
                    if (var.getType() instanceof CollectionType)
                    {
                        // in-mem only
                        operator.setExecutionDataSourceName(child.getExecutionDataSourceName());
                        return;
                    }
                }
            }
            
        }
        
        if (sources.isEmpty()) return;
        
        String src_0 = sources.get(0);
        for (String src : sources)
        {
            try
            {
                StorageSystem src_0_system = m_uas.getDataSource(src_0).getMetaData().getStorageSystem();
                StorageSystem src_system = m_uas.getDataSource(src).getMetaData().getStorageSystem();
                
                if (!src_0_system.equals(src_system))
                {
                    operator.setExecutionDataSourceName(src_0_system.equals(StorageSystem.JDBC) ? src_0 : src);
                }
            }
            catch (DataSourceException e)
            {
                throw new AssertionError(e);
            }
        }
        
        if (operator.getExecutionDataSourceName() == null) // Not distributed
        {
            operator.setExecutionDataSourceName(src_0);
        }
    }
    
    /**
     * Adds mappings from the output variables of the input operator to absolute variables.
     * 
     * @param operator
     *            an operator.
     * @param vars
     *            the list of variables
     */
    private void addNewVariableMappings(Operator operator, Set<? extends Term> vars)
    {
        for (Term t : vars)
        {
            if (!m_vars_to_sources.containsKey(t))
            {
                getSourceOfTerm(t, operator);
            }
        }
    }
    
    private String getSourceOfTerm(Term term, Operator operator)
    {
        
        if (term instanceof AbsoluteVariable)
        {
            m_vars_to_sources.put(term, ((AbsoluteVariable) term).getDataSourceName());
            // m_vars_to_sources.put(out_var, ((AbsoluteVariable) term).getDataSourceName());
            return ((AbsoluteVariable) term).getDataSourceName();
        }
        else if (term instanceof QueryPath)
        {
            if (!m_vars_to_sources.containsKey(term))
            {
                String source = getSourceOfTerm(((QueryPath) term).getTerm(), operator);
                m_vars_to_sources.put(term, source);
                return source;
            }
            else return m_vars_to_sources.get(term);
        }
        else if (term instanceof RelativeVariable)
        {
            Term provenance_term = operator.getOutputInfo().getProvenanceTerm((RelativeVariable) term);
            if (provenance_term instanceof QueryPath)
            {
                String source = getSourceOfTerm((QueryPath) provenance_term, operator);
                m_vars_to_sources.put(term, source);
                m_vars_to_sources.put(provenance_term, source);
                return source;
            }
            else if (provenance_term instanceof RelativeVariable)
            {
                if (((RelativeVariable) provenance_term).getName().equals(Project.PROJECT_ALIAS))
                {
                    // Special treatment for the element variable outputed by a subquery
                    if (operator instanceof Subquery)
                    {
                        assert (operator.getExecutionDataSourceName() != null);
                        m_vars_to_sources.put(term, operator.getExecutionDataSourceName());
                        return m_vars_to_sources.get(term);
                    }
                    else
                    {
                        // Special treatment for the project variable: checks that all the project terms have the same source and
                        // return this source
                        assert (operator instanceof Project);
                        String project_item_source = null;
                        for (edu.ucsd.forward.query.logical.Project.Item item : ((Project) operator).getProjectionItems())
                        {
                            String source = getSourceOfTerm(item.getTerm(), operator);
                            if (project_item_source != null)
                            {
                                assert (source.equals(project_item_source));
                            }
                            else
                            {
                                project_item_source = source;
                            }
                        }
                        m_vars_to_sources.put(term, project_item_source);
                        return m_vars_to_sources.get(term);
                    }
                }
                else
                {
                    assert (m_vars_to_sources.containsKey(provenance_term));
                    m_vars_to_sources.put(term, m_vars_to_sources.get(provenance_term));
                    return m_vars_to_sources.get(provenance_term);
                }
            }
            else if (provenance_term instanceof FunctionCall)
            {
                // if (!((FunctionCall) provenance_term).getFunction().isSqlCompliant())
                // {
                if (provenance_term instanceof ExternalFunctionCall)
                {
                    m_vars_to_sources.put(term, ((ExternalFunctionCall) provenance_term).getFunction().getTargetDataSource());
                    return ((ExternalFunctionCall) provenance_term).getFunction().getTargetDataSource();
                }
                m_vars_to_sources.put(term, DataSource.MEDIATOR);
                // m_vars_to_sources.put(out_var, DataSource.MEDIATOR);
                return DataSource.MEDIATOR;
                // }
                // else
                // {
                // // FIXME What is the source of a function?
                // }
            }
            else if (provenance_term instanceof Constant)
            {
                m_vars_to_sources.put(term, DataSource.MEDIATOR);
                return DataSource.MEDIATOR;
            }
            else if (provenance_term instanceof Parameter)
            {
                m_vars_to_sources.put(term, DataSource.MEDIATOR);
                return DataSource.MEDIATOR;
            }
            else if (provenance_term instanceof AbsoluteVariable)
            {
                // assert (provenance_term instanceof AbsoluteVariable);
                m_vars_to_sources.put(provenance_term, ((AbsoluteVariable) provenance_term).getDataSourceName());
                m_vars_to_sources.put(term, ((AbsoluteVariable) provenance_term).getDataSourceName());
                // m_vars_to_sources.put(out_var, ((AbsoluteVariable) provenance_term).getDataSourceName());
                return ((AbsoluteVariable) provenance_term).getDataSourceName();
            }
            else
            {
                assert (operator.getExecutionDataSourceName() != null);
                m_vars_to_sources.put(term, operator.getExecutionDataSourceName());
                m_vars_to_sources.put(provenance_term, operator.getExecutionDataSourceName());
                return operator.getExecutionDataSourceName();
            }
        }
        else
        {
            return DataSource.MEDIATOR;
        }
    }
    
    @Override
    public Operator visitAssign(Assign operator)
    {
        LogicalPlanDistributorDefault distributor = LogicalPlanDistributorDefault.getInstance(m_uas);
        distributor.m_vars_to_sources = new HashMap<Term, String>(m_vars_to_sources);
        distributor.m_enclosing_plan.addAll(this.m_enclosing_plan);
        distributor.annotate(operator.getLogicalPlansUsed().get(0));
        
        Operator root = operator.getLogicalPlansUsed().get(0).getRootOperator();
        
        if (root.getExecutionDataSourceName() == null)
        {
            assert (root instanceof Ground);
            root.setExecutionDataSourceName(DataSource.MEDIATOR);
            LogicalPlanUtil.setExecutionDataSourceName(operator, DataSource.MEDIATOR, m_uas);
        }
        else
        {
            LogicalPlanUtil.setExecutionDataSourceName(operator, root.getExecutionDataSourceName(), m_uas);
        }
        
        addNewVariableMappings(operator,
                               Collections.singleton(new AbsoluteVariable(DataSource.TEMP_ASSIGN_SOURCE, operator.getTarget())));
        
        return operator;
    }
    
    @Override
    public Operator visitSubquery(Subquery operator)
    {
        if (operator.getOrderVariable() != null)
        {
            operator.setExecutionDataSourceName(DataSource.MEDIATOR);
        }
        else
        {
            operator.setExecutionDataSourceName(operator.getChild().getExecutionDataSourceName());
        }
        addNewVariableMappings(operator, operator.getOutputInfo().getVariables());
        return operator;
    }
    
}
