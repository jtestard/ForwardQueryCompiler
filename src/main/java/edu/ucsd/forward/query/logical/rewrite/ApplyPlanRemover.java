/**
 * 
 */
package edu.ucsd.forward.query.logical.rewrite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.ast.SetQuantifier;
import edu.ucsd.forward.query.function.FunctionRegistryException;
import edu.ucsd.forward.query.function.aggregate.AggregateFunctionCall;
import edu.ucsd.forward.query.function.aggregate.NestFunction;
import edu.ucsd.forward.query.function.collection.CollectionInitFunction;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.logical.ApplyPlan;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.logical.EliminateDuplicates;
import edu.ucsd.forward.query.logical.GroupBy;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.OuterJoin;
import edu.ucsd.forward.query.logical.OuterJoin.Variation;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.plan.LogicalPlanUtil;
import edu.ucsd.forward.query.logical.term.Constant;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.util.NameGenerator;
import edu.ucsd.forward.util.NameGeneratorFactory;

/**
 * @author Yupeng Fu
 * 
 */
public final class ApplyPlanRemover
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ApplyPlanRemover.class);
    
    private LogicalPlan         m_logical_plan;
    
    private boolean             m_monolithic;
    
    private ApplyPlanRemover(boolean monolithic)
    {
        m_monolithic = monolithic;
    }
    
    public static ApplyPlanRemover create(boolean monolithic)
    {
        return new ApplyPlanRemover(monolithic);
    }
    
    /**
     * Traverses the input logical plan bottom up and replaces ApplyPlan operators by GroupBy operators with NEST.
     * 
     * @param plan
     *            the logical plan to visit.
     * @return the logical plan without any ApplyPlan operators.
     * @throws QueryCompilationException
     *             when anything wrong happens
     * @throws QueryExecutionException
     * @throws FunctionRegistryException
     */
    public LogicalPlan rewrite(LogicalPlan plan)
            throws QueryCompilationException, QueryExecutionException, FunctionRegistryException
    {
        m_logical_plan = plan;
        
        assert !(plan.getRootOperator() instanceof ApplyPlan);
        Stack<Operator> stack = new Stack<Operator>();
        LogicalPlanUtil.stackBottomUpRightToLeft(plan.getRootOperator(), stack);
        
        while (!stack.isEmpty())
        {
            Operator operator = stack.pop();
            if (operator instanceof ApplyPlan)
            {
                this.removeApplyPlan((ApplyPlan) operator);
            }
        }
        
        return plan;
    }
    
    private void removeApplyPlan(ApplyPlan apply)
            throws QueryCompilationException, QueryExecutionException, FunctionRegistryException
    {
        // replace apply with LOJ
        Operator applyplan_child = apply.getChild();
        apply.removeChild(applyplan_child);
        Operator applyplan_parent = apply.getParent();
        applyplan_parent.removeChild(apply);
        OuterJoin outerjoin = new OuterJoin(Variation.LEFT);
        String apply_alias = apply.getAlias();
        
        Set<Parameter> nested_parameters = RewriterUtil.collectParameters(apply.getApplyPlan());
        // handle the uncorrelated case
        if (nested_parameters.isEmpty())
        {
            applyplan_parent.addChild(outerjoin);
            // add apply plan child
            outerjoin.addChild(applyplan_child);
            // add rewritten apply plan
            Operator rewritten = rewriteUncorrelatedApplyPlan(apply);
            outerjoin.addChild(rewritten);
            outerjoin.addCondition(new Constant(new BooleanValue(true)));
            outerjoin.updateOutputInfo();
            LogicalPlanUtil.updateAncestorOutputInfo(outerjoin);
            return;
        }
        
        // add the apply-plan child in Assign
        String assign_target = NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.TEMP_TABLE_GENERATOR);
        Assign assign = new Assign(new LogicalPlan(applyplan_child), assign_target);
        LogicalPlanUtil.updateAssignedTempSchemaObject(assign);
        m_logical_plan.addAssignOperator(assign);
        
        // add scan as left argument of LOJ that scans the target of Assign
        Map<Parameter, RelativeVariable> left_mapping = new HashMap<Parameter, RelativeVariable>();
        Scan scan_assign = RewriterUtil.createScanOnAssignedObject(assign, nested_parameters, left_mapping, false);
        outerjoin.addChild(0, scan_assign);
        
        // create Scan and DuplicateEliminate to nested plan
        Map<Parameter, RelativeVariable> right_mapping = new HashMap<Parameter, RelativeVariable>();
        Operator rewritten = RewriterUtil.createScanOnAssignedObject(assign, nested_parameters, right_mapping, true);
        rewritten.updateOutputInfo();
        
        if (m_monolithic)
        {
            EliminateDuplicates eliminate_duplicates = new EliminateDuplicates();
            eliminate_duplicates.addChild(rewritten);
            eliminate_duplicates.updateOutputInfo();
            rewritten = eliminate_duplicates;
        }
        
        // rewrite the applied plan
        rewritten = RewriterUtil.rewriteCorrelatedApplyPlan(apply, rewritten, right_mapping);
        outerjoin.addChild(rewritten);
        // build the join condition
        GeneralFunctionCall condition = RewriterUtil.buildOuterJoinCondition(left_mapping, right_mapping);
        outerjoin.addCondition(condition);
        outerjoin.updateOutputInfo();
        
        // Add the function eval that casts null to empty collection
        CollectionInitFunction function = new CollectionInitFunction();
        RelativeVariable var = new RelativeVariable("__" + apply_alias);
        GeneralFunctionCall call = new GeneralFunctionCall(function, Arrays.<Term> asList(var));
        // FIXME Romain: Yupeng, I commented out the following lines because we don't use the FunctionEval operator anymore.
        // FunctionEval function_eval = new FunctionEval(call, apply_alias);
        // function_eval.addChild(outerjoin);
        // function_eval.updateOutputInfo();
        // applyplan_parent.addChild(function_eval);
        
        LogicalPlanUtil.updateAncestorOutputInfo(outerjoin);
    }
    
    /**
     * Rewrites the uncorrelated apply plan returns the root operator of the rewritten plan.
     * 
     * @param apply
     *            the uncorrelated apply plan
     * @return the root operator of the rewritten plan.
     * @throws QueryCompilationException
     *             anything wrong happens
     * @throws QueryExecutionException
     * @throws FunctionRegistryException
     */
    protected Operator rewriteUncorrelatedApplyPlan(ApplyPlan apply)
            throws QueryCompilationException, QueryExecutionException, FunctionRegistryException
    {
        LogicalPlan nested = apply.getApplyPlan();
        
        // remove apply plans from the nested
        nested = ApplyPlanRemover.create(m_monolithic).rewrite(nested);
        
        // Add GroupBy nest
        Operator nested_root = nested.getRootOperator();
        GroupBy groupby_nest = new GroupBy();
        groupby_nest.setExecutionDataSourceName(DataSource.MEDIATOR);
        groupby_nest.addChild(nested_root);
        
        List<Term> aggs = new ArrayList<Term>(nested_root.getOutputInfo().getVariables());
        groupby_nest.addAggregate(new AggregateFunctionCall(new NestFunction(), SetQuantifier.ALL, aggs), apply.getAlias());
        groupby_nest.updateOutputInfo();
        
        return groupby_nest;
    }
}
