/**
 * 
 */
package edu.ucsd.forward.query.logical.rewrite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.AbstractFunctionCall;
import edu.ucsd.forward.query.function.FunctionRegistryException;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.logical.ApplyPlan;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.logical.EliminateDuplicates;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.OuterJoin;
import edu.ucsd.forward.query.logical.OuterJoin.Variation;
import edu.ucsd.forward.query.logical.Project;
import edu.ucsd.forward.query.logical.Project.Item;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.Select;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.plan.LogicalPlanUtil;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.util.NameGenerator;
import edu.ucsd.forward.util.NameGeneratorFactory;

/**
 * @author Yupeng Fu
 * 
 */
public final class SemiJoinReducer
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(SemiJoinReducer.class);
    
    private LogicalPlan         m_logical_plan;
    
    private SemiJoinReducer()
    {
        
    }
    
    public static SemiJoinReducer create()
    {
        return new SemiJoinReducer();
    }
    
    public LogicalPlan rewrite(LogicalPlan logical_plan)
            throws QueryCompilationException, QueryExecutionException, FunctionRegistryException
    {
        this.m_logical_plan = logical_plan;
        
        assert !(logical_plan.getRootOperator() instanceof ApplyPlan);
        Stack<Operator> stack = new Stack<Operator>();
        LogicalPlanUtil.stackBottomUpRightToLeft(logical_plan.getRootOperator(), stack);
        
        while (!stack.isEmpty())
        {
            Operator operator = stack.pop();
            if (operator instanceof ApplyPlan)
            {
                this.rewrite((ApplyPlan) operator);
            }
        }
        
        return logical_plan;
    }
    
    private void rewrite(Operator operator) throws QueryExecutionException, QueryCompilationException, FunctionRegistryException
    {
        // FIXME: now it only supports apply-plan
        assert operator instanceof ApplyPlan;
        
        ApplyPlan apply = (ApplyPlan) operator;
        
        Set<Parameter> nested_parameters = RewriterUtil.collectParameters(apply.getApplyPlan());
        
        if (nested_parameters.isEmpty())
        {
            // do nothing.
            return;
        }
        
        Operator applyplan_child = apply.getChild();
        apply.removeChild(applyplan_child);
        Operator applyplan_parent = apply.getParent();
        applyplan_parent.removeChild(apply);
        OuterJoin outerjoin = new OuterJoin(Variation.LEFT);
        applyplan_parent.addChild(outerjoin);
        
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
        Scan nested_scan_assign = RewriterUtil.createScanOnAssignedObject(assign, nested_parameters, right_mapping, true);
        EliminateDuplicates assign_duplicate = new EliminateDuplicates();
        assign_duplicate.addChild(nested_scan_assign);
        assign_duplicate.updateOutputInfo();
        apply.addChild(assign_duplicate);
        
        // update parameters
        updateParameters(apply.getApplyPlan(), right_mapping);
        apply.updateOutputInfo();
        outerjoin.addChild(apply);
        
        // build the join condition
        GeneralFunctionCall condition = RewriterUtil.buildOuterJoinCondition(left_mapping, right_mapping);
        outerjoin.addCondition(condition);
        outerjoin.updateOutputInfo();
        
        LogicalPlanUtil.updateAncestorOutputInfo(outerjoin);
        
    }
    
    private void updateParameters(LogicalPlan plan, Map<Parameter, RelativeVariable> mapping) throws QueryCompilationException
    {
        Operator root = plan.getRootOperator();
        for (Operator operator : root.getDescendantsAndSelf())
        {
            for (LogicalPlan nested_plan : operator.getLogicalPlansUsed())
            {
                updateParameters(nested_plan, mapping);
            }
            
            if (operator instanceof Select)
            {
                // rewrite the condition
                updateParameter(((Select) operator).getMergedCondition(), mapping);
            }
            else if (operator instanceof Project)
            {
                Project project = (Project) operator;
                // update all the parameters in the project item
                for (Item item : project.getProjectionItems())
                {
                    if (item.getTerm() instanceof Parameter)
                    {
                        Parameter parameter = (Parameter) item.getTerm();
                        Parameter new_parameter = new Parameter(mapping.get(parameter), parameter.getId());
                        item.setTerm(new_parameter);
                    }
                    else
                    {
                        updateParameter(item.getTerm(), mapping);
                    }
                }
            }
            else
            {
                // FIXME: Add support of more operators
                // do nothing
            }
        }
        plan.updateOutputInfoDeep();
    }
    
    private void updateParameter(Term term, Map<Parameter, RelativeVariable> parameter_mapping)
    {
        updateParameter(null, term, parameter_mapping);
    }
    
    @SuppressWarnings("all")
    private void updateParameter(Term parent, Term term, Map<Parameter, RelativeVariable> parameter_mapping)
    {
        if (term instanceof AbstractFunctionCall)
        {
            List<Term> arguments = new ArrayList((List<Term>) ((AbstractFunctionCall) term).getArguments());
            for (Term arg : arguments)
            {
                updateParameter(term, arg, parameter_mapping);
            }
        }
        else if (term instanceof Parameter)
        {
            Parameter parameter = (Parameter) term;
            if (!(parent instanceof AbstractFunctionCall)) return;
            
            RelativeVariable var = (RelativeVariable) parameter_mapping.get(term).copy();
            assert var != null;
            Parameter new_parameter = new Parameter(var, parameter.getId());
            // replace the parameter in the condition with the variable in the output info
            int index = ((AbstractFunctionCall) parent).removeArgument(term);
            ((AbstractFunctionCall) parent).addArgument(index, new_parameter);
        }
    }
}
