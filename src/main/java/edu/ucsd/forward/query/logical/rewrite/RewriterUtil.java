/**
 * 
 */
package edu.ucsd.forward.query.logical.rewrite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import com.google.common.collect.Lists;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.SetQuantifier;
import edu.ucsd.forward.query.function.AbstractFunctionCall;
import edu.ucsd.forward.query.function.FunctionRegistryException;
import edu.ucsd.forward.query.function.aggregate.AggregateFunctionCall;
import edu.ucsd.forward.query.function.aggregate.NestFunction;
import edu.ucsd.forward.query.function.comparison.EqualFunction;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.function.logical.AndFunction;
import edu.ucsd.forward.query.logical.ApplyPlan;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.logical.Ground;
import edu.ucsd.forward.query.logical.GroupBy;
import edu.ucsd.forward.query.logical.OffsetFetch;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.PartitionBy;
import edu.ucsd.forward.query.logical.Product;
import edu.ucsd.forward.query.logical.Project;
import edu.ucsd.forward.query.logical.Project.Item;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.Select;
import edu.ucsd.forward.query.logical.Sort;
import edu.ucsd.forward.query.logical.UnaryOperator;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.plan.LogicalPlanUtil;
import edu.ucsd.forward.query.logical.term.AbsoluteVariable;
import edu.ucsd.forward.query.logical.term.Constant;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.util.NameGenerator;
import edu.ucsd.forward.util.NameGeneratorFactory;

/**
 * @author Yupeng Fu
 * 
 */
public final class RewriterUtil
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(RewriterUtil.class);
    
    private RewriterUtil()
    {
        
    }
    
    /**
     * Traverse a plan and collect all parameters.
     */
    public static Set<Parameter> collectParameters(LogicalPlan plan)
    {
        assert plan != null;
        
        Operator root_curr = null;
        Set<Parameter> nested_parameters = new HashSet<Parameter>();
        Stack<Operator> stack = new Stack<Operator>();
        LogicalPlanUtil.stackBottomUpRightToLeft(plan.getRootOperator(), stack);
        while (!stack.isEmpty())
        {
            root_curr = stack.pop();
            
            if (!root_curr.getFreeParametersUsed().isEmpty())
            {
                nested_parameters.addAll(root_curr.getFreeParametersUsed());
            }
        }
        
        for (Parameter p : nested_parameters)
        {
            if (p.getType() instanceof CollectionType)
            {
                throw new UnsupportedOperationException("do not support collection-type correlation");
            }
        }
        return nested_parameters;
    }
    
    protected static Scan createScanOnAssignedObject(Assign assign, Set<Parameter> parameters,
            Map<Parameter, RelativeVariable> parameter_mapping, boolean correlatedOnly) throws QueryCompilationException
    {
        assert assign != null;
        Operator ground = new Ground();
        ground.updateOutputInfo();
        AbsoluteVariable assign_var = new AbsoluteVariable(DataSource.TEMP_ASSIGN_SOURCE, assign.getTarget());
        Scan scan = new Scan(NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.ALIAS_GENERATOR), assign_var, ground);
        
        // add navigate to scan
        Set<RelativeVariable> all = assign.getPlan().getRootOperator().getOutputInfo().getVariables();
        for (RelativeVariable var : all)
        {
            RelativeVariable new_var = new RelativeVariable(var.getName(), var.getDefaultProjectAlias());
            if (correlatedOnly)
            {
                new_var = new RelativeVariable(NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.VARIABLE_GENERATOR),
                                               var.getDefaultProjectAlias());
            }
            
            // check if the var corresponds to any parameter
            Parameter found = null;
            for (Parameter p : parameters)
            {
                if (p.getVariable().equals(var))
                {
                    found = p;
                    break;
                }
            }
            if (found != null)
            {
                parameter_mapping.put(found, new_var);
            }
            else if (correlatedOnly)
            {
                // do not add non-parameter to correlated scan
                continue;
            }
            
            QueryPath qp = new QueryPath(scan.getTerm(), Collections.singletonList(var.getDefaultProjectAlias()));
            // FIXME Romain: I commented out this during the refactoring, since the scan doesn't have navigations anymore
            // scan.addNavigateVariable(new_var, qp);
        }
        scan.updateOutputInfo();
        return scan;
    }
    
    protected static GeneralFunctionCall buildOuterJoinCondition(Map<Parameter, RelativeVariable> left_mapping,
            Map<Parameter, RelativeVariable> right_mapping) throws FunctionRegistryException
    {
        GeneralFunctionCall condition = null;
        
        for (Parameter parameter : left_mapping.keySet())
        {
            List<Term> args = new ArrayList<Term>();
            args.add(left_mapping.get(parameter).copy());
            args.add(right_mapping.get(parameter).copy());
            GeneralFunctionCall equal = new GeneralFunctionCall(EqualFunction.NAME, args);
            
            if (condition == null)
            {
                condition = equal;
            }
            else
            {
                args = new ArrayList<Term>();
                args.add(condition);
                args.add(equal);
                condition = new GeneralFunctionCall(AndFunction.NAME, args);
            }
        }
        
        return condition;
    }
    
    protected static Operator rewriteCorrelatedApplyPlan(ApplyPlan apply, Operator scan_assign,
            Map<Parameter, RelativeVariable> parameter_mapping) throws QueryCompilationException
    {
        LogicalPlan plan = apply.getApplyPlan();
        
        // find all the lowest operators that have parameters
        List<Operator> lowests = findLowestParameterizedOperators(plan);
        assert lowests.size() == 1 : "limitation: all the parameterized operators should be on a path";
        
        Operator lowest = lowests.get(0);
        assert lowest.getChildren().size() == 1 : "limitation: only supports parameters on unary operator";
        // replace its child operator with a join
        Operator child = lowest.getChildren().get(0);
        lowest.removeChild(child);
        Product product = new Product();
        product.addChild(child);
        product.addChild(scan_assign);
        product.updateOutputInfo();
        lowest.addChild(product);
        
        // rewrite all its ancestor operators
        for (Operator ancestor : Lists.reverse(lowest.getAncestorsAndSelf()))
        {
            if (ancestor instanceof Select)
            {
                // rewrite the condition
                replaceVariable(((Select) ancestor).getMergedCondition(), parameter_mapping);
                ancestor.updateOutputInfo();
            }
            else if (ancestor instanceof Project)
            {
                Project project = (Project) ancestor;
                // update all the parameters in the project item
                for (Item item : project.getProjectionItems())
                {
                    if (item.getTerm() instanceof Parameter)
                    {
                        item.setTerm(parameter_mapping.get(item.getTerm()));
                    }
                    else
                    {
                        replaceVariable(item.getTerm(), parameter_mapping);
                    }
                }
                
                // project all the var for the parameters
                for (Parameter parameter : new HashSet<Parameter>(parameter_mapping.keySet()))
                {
                    RelativeVariable var = parameter_mapping.get(parameter);
                    String alias = NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.VARIABLE_GENERATOR);
                    project.addProjectionItem(var.copy(), alias, true);
                    RelativeVariable new_var = new RelativeVariable(alias);
                    new_var.setType(var.getType());
                    parameter_mapping.put(parameter, new_var);
                }
                project.updateOutputInfo();
            }
            else if (ancestor instanceof GroupBy)
            {
                GroupBy group_by = (GroupBy) ancestor;
                // FIXME: handle the parameters shown in group-by item and aggregates
                // add all the vars in the group-by terms
                for (RelativeVariable var : parameter_mapping.values())
                {
                    group_by.addGroupByTerm((RelativeVariable) var.copy());
                }
                group_by.updateOutputInfo();
            }
            else if (ancestor instanceof Sort)
            {
                // replace with partition-by op
                Sort sort = (Sort) ancestor;
                PartitionBy partition_by = new PartitionBy();
                
                for (RelativeVariable var : parameter_mapping.values())
                {
                    partition_by.addPartitionByTerm((RelativeVariable) var.copy());
                }
                
                partition_by.setSortByItems(sort.getSortItems());
                replace(sort, partition_by, plan);
                partition_by.updateOutputInfo();
            }
            else if (ancestor instanceof OffsetFetch)
            {
                // look for the partition by op
                OffsetFetch offset_fetch = (OffsetFetch) ancestor;
                // FIXME: need more robust search for partition_by operator
                PartitionBy partition_by = (PartitionBy) offset_fetch.getDescendants(PartitionBy.class).toArray()[0];
                String alias = NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.VARIABLE_GENERATOR);
                partition_by.setRankAlias(alias);
                partition_by.updateOutputInfo();
                // FIXME: handle offset
                Term fetch = offset_fetch.getFetch();
                IntegerValue limit = (IntegerValue) ((Constant) fetch).getValue();
                partition_by.setLimit(limit);
                remove(offset_fetch, plan);
                LogicalPlanUtil.updateAncestorOutputInfo(partition_by);
            }
        }
        
        // Add GroupBy nest
        Operator nested_root = plan.getRootOperator();
        GroupBy groupby_nest = new GroupBy();
        for (RelativeVariable var : parameter_mapping.values())
        {
            groupby_nest.addGroupByTerm(var);
        }
        groupby_nest.setExecutionDataSourceName(DataSource.MEDIATOR);
        groupby_nest.addChild(nested_root);
        
        List<Term> aggs = new ArrayList<Term>();
        for (RelativeVariable var : nested_root.getOutputInfo().getVariables())
        {
            if (parameter_mapping.values().contains(var)) continue;
            aggs.add(var);
        }
        groupby_nest.addAggregate(new AggregateFunctionCall(new NestFunction(), SetQuantifier.ALL, aggs), "__" + apply.getAlias());
        groupby_nest.updateOutputInfo();
        
        return groupby_nest;
    }
    
    protected static void replace(UnaryOperator before, UnaryOperator after, LogicalPlan plan)
    {
        Operator parent = before.getParent();
        Operator child = before.getChild();
        before.removeChild(child);
        after.addChild(child);
        if (parent == null)
        {
            // the root
            plan.setRootOperator(after);
        }
        else
        {
            parent.removeChild(before);
            parent.addChild(after);
        }
    }
    
    protected static void remove(UnaryOperator operator, LogicalPlan plan)
    {
        Operator child = operator.getChild();
        operator.removeChild(child);
        Operator parent = operator.getParent();
        if (parent == null)
        {
            plan.setRootOperator(child);
        }
        else
        {
            parent.removeChild(operator);
            parent.addChild(child);
        }
    }
    
    protected static void replaceVariable(Term term, Map<Parameter, RelativeVariable> parameter_mapping)
    {
        replaceVariable(null, term, parameter_mapping);
    }
    
    @SuppressWarnings("all")
    private static void replaceVariable(Term parent, Term term, Map<Parameter, RelativeVariable> parameter_mapping)
    {
        if (term instanceof AbstractFunctionCall)
        {
            List<Term> arguments = new ArrayList((List<Term>) ((AbstractFunctionCall) term).getArguments());
            for (Term arg : arguments)
            {
                replaceVariable(term, arg, parameter_mapping);
            }
        }
        else if (term instanceof Parameter)
        {
            if (!(parent instanceof AbstractFunctionCall)) return;
            
            RelativeVariable var = (RelativeVariable) parameter_mapping.get(term).copy();
            assert var != null;
            // replace the parameter in the condition with the variable in the output info
            int index = ((AbstractFunctionCall) parent).removeArgument(term);
            ((AbstractFunctionCall) parent).addArgument(index, var);
        }
    }
    
    protected static List<Operator> findLowestParameterizedOperators(LogicalPlan plan)
    {
        List<Operator> operators = new ArrayList<Operator>();
        Stack<Operator> stack = new Stack<Operator>();
        LogicalPlanUtil.stackBottomUpRightToLeft(plan.getRootOperator(), stack);
        while (!stack.isEmpty())
        {
            Operator cur = stack.pop();
            
            if (!cur.getFreeParametersUsed().isEmpty())
            {
                boolean lowest = true;
                for (Operator exist : operators)
                {
                    if (exist.getAncestors().contains(cur))
                    {
                        lowest = false;
                        break;
                    }
                }
                if (lowest) operators.add(cur);
            }
        }
        return operators;
    }
}
