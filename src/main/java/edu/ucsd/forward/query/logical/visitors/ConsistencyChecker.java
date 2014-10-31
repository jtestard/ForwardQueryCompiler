package edu.ucsd.forward.query.logical.visitors;

import java.util.Iterator;

import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.NoSqlType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.query.ast.SetOpExpression.SetOpType;
import edu.ucsd.forward.query.function.FunctionCall;
import edu.ucsd.forward.query.logical.AntiSemiJoin;
import edu.ucsd.forward.query.logical.ApplyPlan;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.logical.Copy;
import edu.ucsd.forward.query.logical.EliminateDuplicates;
import edu.ucsd.forward.query.logical.Exists;
import edu.ucsd.forward.query.logical.Ground;
import edu.ucsd.forward.query.logical.GroupBy;
import edu.ucsd.forward.query.logical.Subquery;
import edu.ucsd.forward.query.logical.GroupBy.Aggregate;
import edu.ucsd.forward.query.logical.IndexScan;
import edu.ucsd.forward.query.logical.IndexScan.KeyRangeSpec;
import edu.ucsd.forward.query.logical.InnerJoin;
import edu.ucsd.forward.query.logical.Navigate;
import edu.ucsd.forward.query.logical.OffsetFetch;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.OuterJoin;
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.PartitionBy;
import edu.ucsd.forward.query.logical.Product;
import edu.ucsd.forward.query.logical.Project;
import edu.ucsd.forward.query.logical.Project.Item;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.Select;
import edu.ucsd.forward.query.logical.SemiJoin;
import edu.ucsd.forward.query.logical.SendPlan;
import edu.ucsd.forward.query.logical.SetOperator;
import edu.ucsd.forward.query.logical.Sort;
import edu.ucsd.forward.query.logical.ddl.CreateDataObject;
import edu.ucsd.forward.query.logical.ddl.DropDataObject;
import edu.ucsd.forward.query.logical.dml.Delete;
import edu.ucsd.forward.query.logical.dml.Insert;
import edu.ucsd.forward.query.logical.dml.Update;
import edu.ucsd.forward.query.logical.dml.Update.Assignment;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.term.AbsoluteVariable;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;

/**
 * The operator visitor that checks the consistency of the operators in a logical plan. Consistency checks make sure that an
 * operator has the expected number of child operators, all the arguments of the operator are valid, the output type of all child
 * operators is a collection type, and that all query paths used by the operator are in scope, that is, they appear in the output
 * info of the child operators. It also checks that logical plans nested within send plan operators are compliant with the data
 * model of the target data source.
 * 
 * Note: The purpose of this class is to assist development of rewriting rules. There is no need for this class to be used in
 * production mode.
 * 
 * @author Michalis Petropoulos
 * 
 */
public final class ConsistencyChecker extends AbstractOperatorVisitor
{
    private UnifiedApplicationState m_uas;
    
    /**
     * Hidden constructor.
     * 
     * @param uas
     *            the unified application state.
     */
    private ConsistencyChecker(UnifiedApplicationState uas)
    {
        assert (uas != null);
        m_uas = uas;
    }
    
    /**
     * Gets a new consistency checker instance.
     * 
     * @param uas
     *            the unified application state.
     * 
     * @return a consistency checker instance.
     */
    public static ConsistencyChecker getInstance(UnifiedApplicationState uas)
    {
        return new ConsistencyChecker(uas);
    }
    
    /**
     * Visits and checks the consistency of the operators in a logical plan.
     * 
     * @param plan
     *            the logical plan to check.
     */
    public void check(LogicalPlan plan)
    {
        for (Assign assign : plan.getAssigns())
        {
            assign.accept(this);
        }
        
        plan.getRootOperator().accept(this);
        
        // Make sure the plan is not nested.
        assert (!plan.isNested());
        
        // Check that the logical plan does not have any free parameters.
        assert (plan.getFreeParametersUsed().isEmpty());
    }
    
    /**
     * Performs consistency checks common to all operators in a logical plan and recursively visits the children of the input
     * operator.
     * 
     * @param operator
     *            the operator to check.
     */
    private void checkOperator(Operator operator)
    {
        // Recursively visit the children of the input operator.
        this.visitChildren(operator);
        
        int children_num = operator.getChildren().size();
        
        // No two children are identical
        for (int i = 0; i < children_num; i++)
            for (int j = i + 1; j < children_num; j++)
                assert (!operator.getChildren().get(i).equals(operator.getChildren().get(j)));
        
        // Check that all variables used by the operator have their binding indexes set, their types set, and either they are
        // relative, that is, they appear in the input, absolute, or they appear in a parameter.
        for (Variable variable : operator.getVariablesUsed())
        {
            assert (variable.getType() != null);
            
            if (variable instanceof AbsoluteVariable)
            {
                assert (variable.getBindingIndex() == -1);
                continue;
            }
            
            if (operator instanceof Navigate) break;
            
            assert (variable.getBindingIndex() > -1);
            
            if (!operator.getInputVariables().contains(variable))
            {
                boolean found = false;
                for (Parameter param : operator.getFreeParametersUsed())
                {
                    if (param.getTerm() == variable)
                    {
                        found = true;
                        break;
                    }
                }
                
                if (!found) assert (false);
            }
        }
        
        // Check the consistency of logical plans passed to the operator as arguments
        for (LogicalPlan logical_plan : operator.getLogicalPlansUsed())
        {
            // Make sure the plan is marked as nested.
            assert (logical_plan.isNested());
            
            for (Assign assign : logical_plan.getAssigns())
            {
                assign.accept(this);
            }
            logical_plan.getRootOperator().accept(this);
        }
    }
    
    /**
     * Performs consistency checks common to all terms in a logical plan given the operator they appear in.
     * 
     * @param term
     *            the term to check.
     * @param operator
     *            the operator the term appears in.
     */
    private void checkTerm(Term term, Operator operator)
    {
        if (term instanceof QueryPath)
        {
            QueryPath query_path = (QueryPath) term;
            // assert (operator instanceof Scan || operator instanceof Navigate || operator instanceof OffsetFetch
            // || operator instanceof Insert || operator instanceof Update || operator instanceof Delete);
            assert (!(query_path.getTerm() instanceof QueryPath));
            this.checkTerm(query_path.getTerm(), operator);
        }
        else if (term instanceof Parameter)
        {
            Parameter param = (Parameter) term;
            assert (param.getTerm() instanceof RelativeVariable || param.getTerm() instanceof QueryPath);
            this.checkTerm(param.getTerm(), operator);
        }
        else if (term instanceof FunctionCall<?>)
        {
            FunctionCall<?> func_call = (FunctionCall<?>) term;
            for (Term arg : func_call.getArguments())
                this.checkTerm(arg, operator);
        }
    }
    
    @Override
    public Operator visitApplyPlan(ApplyPlan operator)
    {
        // Check that there is an apply plan.
        assert (operator.getLogicalPlansUsed().size() == 1);
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitAntiSemiJoin(AntiSemiJoin operator)
    {
        visitInnerJoin(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitCopy(Copy operator)
    {
        // Check that there are no nested copy operators
        assert (operator.getCopyPlan().getRootOperator().getDescendants(Copy.class).size() == 0);
        
        // Check that there are no children
        assert (operator.getChildren().size() == 0);
        
        // Check that the target data source is valid
        assert (operator.getTargetDataSourceName() != null);
        DataSource data_source = null;
        try
        {
            data_source = m_uas.getDataSource(operator.getTargetDataSourceName());
        }
        catch (DataSourceException e)
        {
            assert (false);
        }
        
        // Check that the query result of the nested plan complies with the storage system of the target data source
        for (RelativeVariable var : operator.getCopyPlan().getRootOperator().getOutputInfo().getVariables())
        {
            assert (var.isDataSourceCompliant(data_source.getMetaData()));
        }
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitEliminateDuplicates(EliminateDuplicates operator)
    {
        assert (operator.getChildren().size() == 1);
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitExists(Exists operator)
    {
        // Check that there is an exists plan.
        assert (operator.getLogicalPlansUsed().size() == 1);
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitGround(Ground operator)
    {
        assert (operator.getChildren().size() == 0);
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitGroupBy(GroupBy operator)
    {
        // Make sure the type of the group by terms is scalar.
        for (Term term : operator.getGroupByTerms())
        {
            assert (term.getType() instanceof ScalarType || term.getType() instanceof NoSqlType);
            this.checkTerm(term, operator);
        }
        
        // Check the aggregate function calls
        for (Aggregate agg : operator.getAggregates())
        {
            this.checkTerm(agg.getAggregateFunctionCall(), operator);
        }
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitInnerJoin(InnerJoin operator)
    {
        assert (operator.getChildren().size() >= 2);
        
        // Make sure there are join conditions.
        assert (!operator.getConditions().isEmpty());
        
        // Make sure the type of the join conditions is boolean.
        for (Term condition : operator.getConditions())
        {
            assert (condition.getType() instanceof BooleanType || condition.getType() instanceof NoSqlType);
            this.checkTerm(condition, operator);
        }
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitNavigate(Navigate operator)
    {
        assert (operator.getChildren().size() == 1);
        
        this.checkOperator(operator);
        this.checkTerm(operator.getTerm(), operator);
        
        return operator;
    }
    
    @Override
    public Operator visitOffsetFetch(OffsetFetch operator)
    {
        assert (operator.getChildren().size() == 1);
        
        // Make sure that offset or fetch are not null.
        assert (operator.getOffset() != null || operator.getFetch() != null);
        
        // Make sure the type of offset and fetch is integer.
        if (operator.getOffset() != null)
        {
            assert (operator.getOffset().getType() instanceof IntegerType || operator.getOffset().getType() instanceof NoSqlType);
            this.checkTerm(operator.getOffset(), operator);
        }
        if (operator.getFetch() != null)
        {
            assert (operator.getFetch().getType() instanceof IntegerType || operator.getFetch().getType() instanceof NoSqlType);
            this.checkTerm(operator.getFetch(), operator);
        }
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitOuterJoin(OuterJoin operator)
    {
        visitInnerJoin(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitProduct(Product operator)
    {
        assert (operator.getChildren().size() >= 2);
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitProject(Project operator)
    {
        assert (operator.getChildren().size() == 1);
        
        this.checkOperator(operator);
        // Check the projection items
        for (Item item : operator.getProjectionItems())
        {
            this.checkTerm(item.getTerm(), operator);
        }
        
        return operator;
    }
    
    @Override
    public Operator visitScan(Scan operator)
    {
        assert (operator.getChildren().size() == 1);
        
        this.checkOperator(operator);
        this.checkTerm(operator.getTerm(), operator);
        
        return operator;
    }
    
    @Override
    public Operator visitSelect(Select operator)
    {
        assert (operator.getChildren().size() == 1);
        
        // Make sure the condition is not null.
        assert (operator.getMergedCondition() != null);
        
        // Make sure the condition's type is boolean.
        assert (operator.getMergedCondition().getType() instanceof BooleanType || operator.getMergedCondition().getType() instanceof NoSqlType);
        
        this.checkOperator(operator);
        // Check the conditions
        for (Term term : operator.getConditions())
        {
            this.checkTerm(term, operator);
        }
        
        return operator;
    }
    
    @Override
    public Operator visitSemiJoin(SemiJoin operator)
    {
        visitInnerJoin(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitSendPlan(SendPlan operator)
    {
        // Get the execution data source of the send plan
        DataSource data_source = null;
        try
        {
            data_source = m_uas.getDataSource(operator.getExecutionDataSourceName());
        }
        catch (DataSourceException e)
        {
            assert (false);
        }
        
        switch (data_source.getMetaData().getStorageSystem())
        {
            case INMEMORY:
                // Check that there are no child operators if the execution data source of the send plan is INMEMORY.
                assert (operator.getChildren().size() == 0);
                break;
            default:
                // Check that there are no nested send plan operators and that the root operator is a project if the execution data
                // source of the send plan is not INMEMORY.
                assert (operator.getSendPlan().getRootOperator().getDescendants(SendPlan.class).size() == 0);
                // assert (operator.getSendPlan().getRootOperator() instanceof Project);
                break;
        }
        
        // FIXME The SendPlan operator is supposed to have many Copy operators and at most one non-copy operator
        // Check that all children are copy operators except at most one
        int non_copy = 0;
        for (Operator child : operator.getChildren())
        {
            if (!(child instanceof Copy)) non_copy++;
        }
        assert (non_copy <= 1);
        
        // Check that there is a send plan.
        assert (operator.getLogicalPlansUsed().size() == 1);
        
        // Check that the nested plan complies with the storage system of the target data source
        assert (operator.getSendPlan().isDataSourceCompliant(data_source.getMetaData()));
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitSetOperator(SetOperator operator)
    {
        assert (operator.getChildren().size() == 2);
        
        if (operator.getSetOpType() == SetOpType.OUTER_UNION)
        {
            // Make sure the two children do not output the same attribute name with incompatible types
            OutputInfo left_info = operator.getChildren().get(0).getOutputInfo();
            OutputInfo right_info = operator.getChildren().get(1).getOutputInfo();
            for (RelativeVariable left_var : left_info.getVariables())
            {
                if (right_info.contains(left_var))
                {
                    RelativeVariable right_var = right_info.getVariable(left_var.getName());
                    TypeUtil.deepEqualsByIsomorphism(left_var.getType(), right_var.getType());
                }
            }
        }
        else
        {
            // Make sure both children have the same output type
            Iterator<RelativeVariable> left_iter = operator.getChildren().get(0).getOutputInfo().getVariables().iterator();
            Iterator<RelativeVariable> right_iter = operator.getChildren().get(1).getOutputInfo().getVariables().iterator();
            while (left_iter.hasNext())
            {
                RelativeVariable left_var = left_iter.next();
                Type left_type = left_var.getType();
                RelativeVariable right_var = right_iter.next();
                Type right_type = right_var.getType();
                assert (TypeUtil.deepEqualsByIsomorphism(left_type, right_type));
            }
            assert (!right_iter.hasNext());
        }
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitSort(Sort operator)
    {
        assert (operator.getChildren().size() == 1);
        
        // Check that there is at least one sort item.
        assert (operator.getSortItems().size() > 0);
        
        this.checkOperator(operator);
        // Check the conditions
        for (Sort.Item item : operator.getSortItems())
        {
            this.checkTerm(item.getTerm(), operator);
        }
        
        return operator;
    }
    
    @Override
    public Operator visitCreateSchemaObject(CreateDataObject operator)
    {
        assert (operator.getChildren().size() == 0);
        
        return operator;
    }
    
    @Override
    public Operator visitDropSchemaObject(DropDataObject operator)
    {
        assert (operator.getChildren().size() == 0);
        
        return operator;
    }
    
    @Override
    public Operator visitInsert(Insert operator)
    {
        assert (operator.getChildren().size() == 1);
        
        // Check that there is an insert plan.
        assert (operator.getLogicalPlansUsed().size() == 1);
        
        Term access_term = operator.getTargetTerm();
        this.checkTerm(access_term, operator);
        
        assert (access_term.getType() instanceof CollectionType);
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitDelete(Delete operator)
    {
        assert (operator.getChildren().size() == 1);
        this.checkTerm(operator.getTargetTerm(), operator);
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitUpdate(Update operator)
    {
        assert (operator.getChildren().size() == 1);
        
        if (operator.getTargetTerm() != null)
        {
            this.checkTerm(operator.getTargetTerm(), operator);
        }
        for (Assignment assignment : operator.getAssignments())
        {
            this.checkTerm(assignment.getTerm(), operator);
        }
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitPartitionBy(PartitionBy operator)
    {
        // FIXME What checks are needed?
        // Do nothing
        return operator;
    }
    
    @Override
    public Operator visitIndexScan(IndexScan operator)
    {
        visitScan(operator);
        
        // Make sure there is key range spec.
        assert (!operator.getKeyRangeSpecs().isEmpty());
        
        // Make sure all the ranges are valid.
        for (KeyRangeSpec spec : operator.getKeyRangeSpecs())
            assert spec.getLowerTerm() != null || spec.getUpperTerm() != null;
        
        return operator;
    }
    
    @Override
    public Operator visitAssign(Assign operator)
    {
        // Check that there is an apply plan.
        assert (operator.getLogicalPlansUsed().size() == 1);
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitSubquery(Subquery operator)
    {
        assert (operator.getChildren().size() == 1);
        
        this.checkOperator(operator);
        
        return operator;
    }
}
