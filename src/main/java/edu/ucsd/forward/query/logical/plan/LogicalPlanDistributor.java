/**
 * 
 */
package edu.ucsd.forward.query.logical.plan;

import java.util.Collections;
import java.util.Stack;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;
import edu.ucsd.forward.query.logical.Copy;
import edu.ucsd.forward.query.logical.Ground;
import edu.ucsd.forward.query.logical.Navigate;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.Project;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.SendPlan;
import edu.ucsd.forward.query.logical.SetOperator;
import edu.ucsd.forward.query.logical.Subquery;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.visitors.LogicalPlanDistributorDefault;
import edu.ucsd.forward.util.NameGenerator;
import edu.ucsd.forward.util.NameGeneratorFactory;

/**
 * The front-end for applying all distribution related annotations and rewritings.
 * 
 * More specifically, first the LogicalPlanDistributorDefault is called which annotates operators if their output variables all come
 * from the same sources. The rest of the operators are annotated by the LogicPlanDistributorInferred. Next, the rewriting for
 * DistributedJoins is employed that replaces Left-OuterJoins created from the ApplyPlanRewriting with DistributedOuterJoins. Last,
 * SendPlan and Copy operators are inserted in the plan.
 * 
 * @author <Vicky Papavasileiou>
 * @author Michalis Petropoulos
 * @author Romain Vernoux
 * 
 */
public final class LogicalPlanDistributor
{
    @SuppressWarnings("unused")
    private static final Logger     log                      = Logger.getLogger(LogicalPlanDistributor.class);
    
    private UnifiedApplicationState m_uas;
    
    public static final boolean     distributed_join_enabled = false;
    
    /**
     * Private constructor.
     * 
     * @param uas
     *            the unified application state.
     */
    private LogicalPlanDistributor(UnifiedApplicationState uas)
    {
        assert (uas != null);
        m_uas = uas;
    }
    
    /**
     * Gets an instance of the logical plan distributor.
     * 
     * @param uas
     *            the unified application state.
     * @return an instance of the logical plan distributor.
     */
    public static LogicalPlanDistributor getInstance(UnifiedApplicationState uas)
    {
        return new LogicalPlanDistributor(uas);
    }
    
    /**
     * Distributes the input logical plan by setting the execution data source of all logical operators and creating the send plan
     * operators.
     * 
     * @param logical_plan
     *            the logical plan to distribute.
     * @return a logical plan in distributed normal form.
     * @throws QueryCompilationException
     *             if there is a query compilation error.
     */
    public LogicalPlan distribute(LogicalPlan logical_plan) throws QueryCompilationException
    {
        LogicalPlanDistributorDefault.getInstance(m_uas).annotate(logical_plan);
        
        addSendPlanOperators(logical_plan);
        
        return logical_plan;
    }
    
    /**
     * Visits the logical plan bottom-up and inserts Send Plan and Copy operators.
     * 
     * @param logical_plan
     *            the logical plan for which to create send plan operators.
     * @return a logical plan in distributed normal form.
     * @throws QueryCompilationException
     *             if there is a query compilation error.
     */
    private LogicalPlan addSendPlanOperators(LogicalPlan logical_plan) throws QueryCompilationException
    {
        // Visit Assign operators first
        
        Stack<Operator> stack = new Stack<Operator>();
        for (Assign a : logical_plan.getAssigns())
        {
            stack.push(a);
        }
        
        LogicalPlanUtil.stackBottomUpRightToLeft(logical_plan.getRootOperator(), stack);
        
        Operator root_op = null;
        while (!stack.isEmpty())
        {
            Operator next = stack.pop();
            if (next instanceof Ground)
            {
                Operator parent = next.getParent();
                if (parent != null && parent.getExecutionDataSourceName() != null)
                {
                    next.setExecutionDataSourceName(parent.getExecutionDataSourceName());
                }
            }
            if (!(next instanceof Assign)) root_op = addSendPlanOperator(next);
            
            for (LogicalPlan plan : next.getLogicalPlansUsed())
            {
                addSendPlanOperators(plan);
            }
        }
        
        // Do not create new logical plan object since the existing one might be an argument of an operator
        logical_plan.setRootOperator(root_op);
        logical_plan.updateOutputInfoDeep();
        
        // Sanity check (too expensive to check during production)
        // try
        // {
        // logical_plan.updateOutputInfoDeep();
        // }
        // catch (QueryCompilationException e)
        // {
        // assert (false);
        // }
        
        return logical_plan;
    }
    
    /**
     * Rules for adding SendPlan operators:
     * 
     * The root of a LogicalPlan must be a SendPlan operator.
     * 
     * If the execution source of a child operator is different than the parent's execution source, a SendPlan operator is added in
     * place of the child. The execution source of the SendPlan is the execution source of the child. The plan rooted from the child
     * operator becomes the nested plan of the SendPlan operator.
     * 
     * 
     * @param operator
     *            a logical operator.
     * @return the input logical operator, or a new send plan operator.
     * @throws QueryCompilationException
     *             if there is a query compilation error.
     */
    private Operator addSendPlanOperator(Operator operator) throws QueryCompilationException
    {
        String exec_data_source = operator.getExecutionDataSourceName();
        
        assert (exec_data_source != null);
        
        SendPlan send_plan = null;
        Operator parent = operator.getParent();
        
        // Add the top send plan operator
        if (parent == null)
        {
            assert (!(operator instanceof SendPlan));
            
            // Create the nested plan
            LogicalPlan logical_plan = new LogicalPlan(operator);
            logical_plan.updateOutputInfoShallow();
            send_plan = new SendPlan(exec_data_source, logical_plan);
        }
        else
        {
            String parent_exec_data_source = parent.getExecutionDataSourceName();
            
            // Add an intermediate send plan operator
            if (!exec_data_source.equals(parent_exec_data_source))
            {
                assert (!(operator instanceof SendPlan));
                assert (!(parent instanceof SendPlan));
                
                if (parent instanceof SetOperator || parent instanceof Subquery)
                {
                    // We can directly insert the sendplan at this point without subquery
                    int index = parent.getChildren().indexOf(operator);
                    parent.removeChild(operator);
                    LogicalPlan logical_plan = new LogicalPlan(operator);
                    logical_plan.updateOutputInfoShallow();
                    send_plan = new SendPlan(exec_data_source, logical_plan);
                    parent.addChild(index, send_plan);
                }
                else
                {
                    // Insert a subquery at this point of the plan. The Subquery stays above, the child goes in the send plan
                    SubqueryRewriter subquery_rewriter = new SubqueryRewriter(NameGenerator.DISTRIBUTOR_VAR_GENERATOR);
                    Subquery subquery = subquery_rewriter.insertSubquery(operator);
                    LogicalPlanUtil.updateAncestorOutputInfo(subquery.getChild());
                    
                    Operator child = subquery.getChild();
                    
                    subquery.removeChild(child);
                    
                    // Create the nested plan
                    LogicalPlan logical_plan = new LogicalPlan(child);
                    logical_plan.updateOutputInfoShallow();
                    send_plan = new SendPlan(exec_data_source, logical_plan);
                    subquery.addChild(send_plan);
                }
            }
        }
        
        if (send_plan != null)
        {
            addCopyOperators(send_plan);
            
            try
            {
                LogicalPlanUtil.updateAncestorOutputInfo(send_plan);
            }
            catch (QueryCompilationException e)
            {
                assert (false);
            }
            
            return send_plan;
        }
        else
        {
            return operator;
        }
    }
    
    /**
     * Adds Copy operators for each nested SendPlan operator.
     * 
     * Rules for adding Copy operators:
     * 
     * The execution data source of the Send Plan operator must not be the mediator and the nested plan must contain a Send Plan
     * operator.
     * 
     * When a Copy operator is added, the nested plan rooted at the nested Send Plan becomes the nested plan of the Copy. The
     * execution source of the Copy is always the mediator. The copy operator becomes the child of the parent Send Plan whereas in
     * place of the nested Send Plan, a new Scan that iterates over a Parameter is inserted. The Parameter has the type of the
     * output type of the Copy plan.
     * 
     * If the OutputInfo of the Copy contains variables that are not of type Scalar, a Project needs to be inserted that projects
     * only scalar types. This is required because the output tuples of the Copy plan will be inserted into a database.
     * 
     * @param operator
     *            the root SendPlan operator.
     * @throws QueryCompilationException
     *             if there is a query compilation error.
     */
    private void addCopyOperators(SendPlan operator) throws QueryCompilationException
    {
        // Do not introduce copy operators if the execution data source of the send plan operator is an INMEMORY one.
        DataSourceMetaData metadata = null;
        try
        {
            metadata = m_uas.getDataSource(operator.getExecutionDataSourceName()).getMetaData();
        }
        catch (DataSourceException e)
        {
            throw new AssertionError(e);
        }
        switch (metadata.getStorageSystem())
        {
            case INMEMORY:
                return;
            default:
                break;
        }
        
        // For each nested send-plan operator
        for (SendPlan nested_send_plan : operator.getSendPlan().getRootOperator().getDescendants(SendPlan.class))
        {
            if (!nested_send_plan.getExecutionDataSourceName().equals(DataSource.MEDIATOR))
            {
                assert (nested_send_plan.getOutputInfo().size() > 0);
            }
            
            Operator parent = nested_send_plan.getParent();
            int index = parent.getChildren().indexOf(nested_send_plan);
            
            // Detach the send plan operator
            parent.removeChild(nested_send_plan);
            
            // Create the nested plan
            LogicalPlan logical_plan = new LogicalPlan(nested_send_plan);
            logical_plan.updateOutputInfoShallow();
            
            // Attach a new copy operator
            String alias = NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.DISTRIBUTOR_VAR_GENERATOR);
            Copy copy = new Copy(operator.getExecutionDataSourceName(), alias, logical_plan);
            copy.setExecutionDataSourceName(DataSource.MEDIATOR);
            copy.updateOutputInfo();
            
            operator.addChild(copy);
            
            // Replace plan of copy operator with Project-Scan-Ground
            
            // Attach a new scan operator scanning a parameter
            RelativeVariable variable = copy.getOutputInfo().getVariable(alias);
            Parameter param = new Parameter(variable, operator.getExecutionDataSourceName(), alias);
            
            Operator ground = new Ground();
            ground.updateOutputInfo();
            ground.setExecutionDataSourceName(operator.getExecutionDataSourceName());
            ground.setCardinalityEstimate(Size.ONE);
            
            Scan scan = new Scan(alias, param, ground);
            scan.updateOutputInfo();
            scan.setExecutionDataSourceName(operator.getExecutionDataSourceName());
            scan.setCardinalityEstimate(nested_send_plan.getCardinalityEstimate());
            
            Project project = new Project();
            assert scan.getOutputInfo().getVariables().size() == 1;
            Type scan_type = scan.getOutputInfo().getVariables().iterator().next().getType();
            Operator nav_on_top = scan;
            if (scan_type instanceof TupleType)
            {
                for (String attr_name : ((TupleType) scan_type).getAttributeNames())
                {
                    String fresh_alias = NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.DISTRIBUTOR_VAR_GENERATOR);
                    QueryPath qp = new QueryPath(scan.getAliasVariable(), Collections.singletonList(attr_name));
                    Navigate nav = new Navigate(fresh_alias, qp);
                    nav.updateOutputInfo();
                    nav.setExecutionDataSourceName(operator.getExecutionDataSourceName());
                    nav.setCardinalityEstimate(scan.getCardinalityEstimate());
                    LogicalPlanUtil.insertOnTop(nav_on_top, nav);
                    nav_on_top = nav;
                    project.addProjectionItem(nav.getAliasVariable(), attr_name, true);
                }
            }
            else
            {
                // We don't distribute queries with SELECT ELEMENT
                throw new UnsupportedOperationException();
            }
            
            project.addChild(nav_on_top);
            project.updateOutputInfo();
            project.setExecutionDataSourceName(operator.getExecutionDataSourceName());
            project.setCardinalityEstimate(scan.getCardinalityEstimate());
            
            parent.addChild(index, project);
            
            LogicalPlanUtil.updateAncestorOutputInfo(scan);
            operator.getSendPlan().updateOutputInfoShallow();
        }
    }
}
