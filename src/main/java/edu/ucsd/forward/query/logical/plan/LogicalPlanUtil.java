/**
 * 
 */
package edu.ucsd.forward.query.logical.plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.SwitchType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.TupleType.AttributeEntry;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.JsonValue;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.SwitchValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.TupleValue.AttributeValueEntry;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AttributeReference;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.ast.function.AbstractFunctionNode;
import edu.ucsd.forward.query.function.FunctionCall;
import edu.ucsd.forward.query.function.FunctionRegistryException;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.function.logical.AndFunction;
import edu.ucsd.forward.query.logical.AbstractOperator;
import edu.ucsd.forward.query.logical.AbstractUnaryOperator;
import edu.ucsd.forward.query.logical.ApplyPlan;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;
import edu.ucsd.forward.query.logical.Exists;
import edu.ucsd.forward.query.logical.Ground;
import edu.ucsd.forward.query.logical.InnerJoin;
import edu.ucsd.forward.query.logical.Navigate;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.Project;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.Select;
import edu.ucsd.forward.query.logical.SendPlan;
import edu.ucsd.forward.query.logical.SetOperator;
import edu.ucsd.forward.query.logical.Subquery;
import edu.ucsd.forward.query.logical.UnaryOperator;
import edu.ucsd.forward.query.logical.dml.Update;
import edu.ucsd.forward.query.logical.term.AbsoluteVariable;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.util.NameGenerator;

/**
 * Utility methods for logical plan builder.
 * 
 * @author <Vicky Papavasileiou>
 * @author Michalis Petropoulos
 * 
 */
public final class LogicalPlanUtil
{
    /**
     * Hidden constructor.
     */
    private LogicalPlanUtil()
    {
    }
    
    /**
     * Determines whether all input variables are absolute or not.
     * 
     * @param variables
     *            a collection of variables.
     * @return true, if all input variables are absolute; otherwise, false.
     */
    public static boolean areAbsoluteVariables(Collection<Variable> variables)
    {
        for (Variable var : variables)
        {
            if (!(var instanceof AbsoluteVariable)) return false;
        }
        return true;
    }
    
    /**
     * Gets the collection of absolute variables in the input collection of variables.
     * 
     * @param variables
     *            a collection of variables.
     * @return the collection of absolute variables in the input collection of variables.
     */
    public static List<AbsoluteVariable> getAbsoluteVariables(Collection<Variable> variables)
    {
        List<AbsoluteVariable> out = new ArrayList<AbsoluteVariable>();
        
        for (Variable var : variables)
        {
            if (var instanceof AbsoluteVariable) out.add((AbsoluteVariable) var);
        }
        return out;
    }
    
    /**
     * Determines whether all input variables are relative or not.
     * 
     * @param variables
     *            a collection of variables.
     * @return true, if all input variables are relative; otherwise, false.
     */
    public static boolean areRelativeVariables(Collection<Variable> variables)
    {
        if (variables.isEmpty()) return false;
        
        for (Variable var : variables)
        {
            if (var instanceof AbsoluteVariable) return false;
        }
        
        return true;
    }
    
    /**
     * Gets the collection of relative variables in the input collection of variables.
     * 
     * @param variables
     *            a collection of variables.
     * @return the collection of relative variables in the input collection of variables.
     */
    public static List<RelativeVariable> getRelativeVariables(Collection<Variable> variables)
    {
        List<RelativeVariable> out = new ArrayList<RelativeVariable>();
        
        for (Variable var : variables)
        {
            if (var instanceof RelativeVariable) out.add((RelativeVariable) var);
        }
        return out;
    }
    
    /**
     * 
     * @param start_type
     *            The type that is being scanned. Can either be TupleType,CollectionType
     * @param steps
     *            The QueryPath that is being traversed
     * @return the type of the last path step
     */
    @SuppressWarnings("unchecked")
    public static Type getTypeOfAttribute(Type start_type, List<String> steps)
    {
        if (start_type instanceof TupleType || start_type instanceof CollectionType)
        {
            Iterator<AttributeEntry> it = ((Iterable<AttributeEntry>) start_type).iterator();
            while (it.hasNext())
            {
                AttributeEntry next = it.next();
                Type end_type = null;
                if (next.getName().equals(steps.get(0)))
                {
                    if (steps.size() == 1)
                    {
                        return next.getType();
                    }
                    steps.remove(0);
                    end_type = getTypeOfAttribute(next.getType(), steps);
                    if (end_type != null) return end_type;
                }
                
            }
        }
        else if (start_type instanceof ScalarType)
        {
            if (steps.size() == 0)
            {
                return start_type;
            }
        }
        else if (start_type instanceof SwitchType)
        {
            for (String case_name : ((SwitchType) start_type).getCaseNames())
            {
                if (case_name.equals(steps.get(0)))
                {
                    if (steps.size() == 1)
                    {
                        return ((SwitchType) start_type).getCase(case_name);
                    }
                    steps.remove(0);
                    Type end_type = getTypeOfAttribute(((SwitchType) start_type).getCase(case_name), steps);
                    if (end_type != null) return end_type;
                    
                }
            }
        }
        return null;
    }
    
    public static List<AttributeReference> getAllAttributesOfFunction(AbstractFunctionNode f, List<AttributeReference> list)
    {
        
        for (ValueExpression arg : f.getArguments())
        {
            if (arg instanceof AttributeReference)
            {
                list.add((AttributeReference) arg);
            }
            else if (arg instanceof AbstractFunctionNode)
            {
                getAllAttributesOfFunction((AbstractFunctionNode) arg, list);
            }
        }
        return list;
    }
    
    /**
     * Return the value of the attribute with the given name. The attribute can be a ScalarValue, TupleValue, CollectionValue,
     * JavaValue or JsonValue.
     * 
     * @param start_value
     *            The TupleValue that contains the attributes
     * @param name
     *            The attribute name to look for
     * @return The value of the attribute with the given name
     */
    @SuppressWarnings("unchecked")
    public static Value getValueOfAttribute(Value start_value, List<String> steps)
    {
        if (start_value instanceof TupleValue || start_value instanceof CollectionValue)
        {
            Iterator<AttributeValueEntry> it = ((Iterable<AttributeValueEntry>) start_value).iterator();
            while (it.hasNext())
            {
                AttributeValueEntry next = it.next();
                Value end = null;
                if (next.getName().equals(steps.get(0)))
                {
                    if (steps.size() == 1)
                    {
                        return next.getValue();
                    }
                    steps.remove(0);
                    end = getValueOfAttribute(next.getValue(), steps);
                    if (end != null) return end;
                }
            }
        }
        else if (start_value instanceof ScalarValue)
        {
            if (steps.size() == 0)
            {
                return start_value;
            }
        }
        else if (start_value instanceof SwitchValue)
        {
            if (steps.get(0).equals(((SwitchValue) start_value).getCaseName()))
            {
                if (steps.size() == 1)
                {
                    return ((SwitchValue) start_value).getCase();
                }
                steps.remove(0);
                Value end = getValueOfAttribute(((SwitchValue) start_value).getCase(), steps);
                if (end != null) return end;
            }
        }
        else if (start_value instanceof JsonValue)
        {
            
        }
        return null;
    }
    
    /**
     * Finds the lowest common ancestor in the logical plan tree rooted at the input operator that provides all input variables.
     * 
     * @param operator
     *            the root of the tree to search.
     * @param variables
     *            a collection of relative variables.
     * @return the lowest common ancestor, or null if there isn't one.
     */
    public static Operator getLowestCommonAncestor(Operator operator, Collection<RelativeVariable> variables)
    {
        
        if (operator.getOutputInfo().getVariables().containsAll(variables) || operator.getInputVariables().containsAll(variables))
        {
            Operator lca = operator;
            // Set Op redefines the variables. Do not proceed more.
            if (operator instanceof SetOperator) return operator;
            for (Operator child : operator.getChildren())
            {
                Operator child_lca = getLowestCommonAncestor(child, variables);
                if (child_lca != null)
                {
                    lca = child_lca;
                }
            }
            
            return lca;
        }
        else
        {
            if (variables.size() == 1)
            {
                RelativeVariable var = variables.iterator().next();
                return getDefininingScanOperator(operator, var);
            }
        }
        
        return null;
    }
    
    /**
     * Returns the first (top-down) operator of a given class in a specified (unary) operator tree.
     * 
     * @param <T>
     *            the type of operator
     * @param root
     *            the root of the tree to look into
     * @param operator_class
     *            the class of operator to look for
     * @return the first operator from the correct class, or <code>null</code> if no operator with the correct class or found or if
     *         a non-unary operator is encountered.
     */
    @SuppressWarnings("unchecked")
    public static <T> T getFirstOperatorWithName(Operator root, Class<T> operator_class)
    {
        Operator current = root;
        while (true)
        {
            if (current.getClass().equals(operator_class))
            {
                return (T) current;
            }
            else if (!(current instanceof UnaryOperator))
            {
                return null;
            }
            current = ((UnaryOperator) current).getChild();
        }
    }
    
    /**
     * Returns the previous (bottom-up) operator that is not an Exists, Navigate or ApplyPlan.
     * 
     * @param root
     *            the bottom of the tree to look into
     * @return the first operator that is not an Exists, Navigate or ApplyPlan, or <code>null</code> if no operator is found or if a
     *         non-unary operator is encountered.
     */
    public static Operator getPreviousClauseOperator(Operator root)
    {
        Operator current = root;
        while (current != null)
        {
            current = current.getParent();
            if (!(current instanceof Exists || current instanceof Navigate || current instanceof ApplyPlan))
            {
                break;
            }
        }
        
        return current;
    }
    
    /**
     * Returns the next (top-down) operator that is not an Exists, Navigate or ApplyPlan, below the given root.
     * 
     * @param root
     *            the root of the operator
     * @return the first operator that is not an Exists, Navigate or ApplyPlan, or <code>null</code> if no operator is found or if a
     *         non-unary operator is encountered.
     */
    public static Operator getNextClauseOperator(Operator root)
    {
        Operator current = root;
        while (true)
        {
            if (!(current instanceof UnaryOperator))
            {
                return null;
            }
            current = ((UnaryOperator) current).getChild();
            if (!(current instanceof Exists || current instanceof Navigate || current instanceof ApplyPlan))
            {
                return current;
            }
        }
    }
    
    /**
     * Returns the next (top-down) operator that is not an Exists, Navigate or ApplyPlan, starting with the root node itself.
     * 
     * @param root
     *            the root of the operator
     * @return the first operator that is not an Exists, Navigate or ApplyPlan, or <code>null</code> if no operator is found or if a
     *         non-unary operator is encountered.
     */
    public static Operator getNextClauseOperatorOrSelf(Operator root)
    {
        Operator current = root;
        while (true)
        {
            if (!(current instanceof Exists || current instanceof Navigate || current instanceof ApplyPlan))
            {
                return current;
            }
            if (!(current instanceof UnaryOperator))
            {
                return null;
            }
            current = ((UnaryOperator) current).getChild();
        }
    }
    
    public static Scan getDefininingScanOperator(Operator root, RelativeVariable var)
    {
        if (root instanceof Scan && root.getOutputInfo().hasProvenanceVariable(var))
        {
            return (Scan) root;
        }
        else
        {
            for (Operator child : root.getChildren())
            {
                Operator child_scan = getDefininingScanOperator(child, var);
                if (child_scan != null)
                {
                    return (Scan) child_scan;
                }
            }
        }
        return null;
    }
    
    public static Project getProjectWithAlias(Operator root, RelativeVariable var)
    {
        if (root instanceof Project)
        {
            String proj_item_alias = ((Project) root).getProjectionItems().get(0).getAlias();
            int prefix_index = proj_item_alias.indexOf(NameGenerator.NAME_PREFIX);
            String project_alias = proj_item_alias.substring(0, prefix_index);
            if (project_alias.equals(var.getName())) return (Project) root;
        }
        else
        {
            for (LogicalPlan plan : root.getLogicalPlansUsed())
            {
                Operator nested_project = getProjectWithAlias(plan.getRootOperator(), var);
                if (nested_project != null)
                {
                    return (Project) nested_project;
                }
            }
            for (Operator child : root.getChildren())
            {
                
                Operator child_project = getProjectWithAlias(child, var);
                if (child_project != null)
                {
                    return (Project) child_project;
                }
                for (LogicalPlan plan : child.getLogicalPlansUsed())
                {
                    Operator nested_project = getProjectWithAlias(plan.getRootOperator(), var);
                    if (nested_project != null)
                    {
                        return (Project) nested_project;
                    }
                }
            }
        }
        return null;
    }
    
    public static Scan getDefininingScanOperatorWithSubPlans(Operator root, RelativeVariable var)
    {
        if (root instanceof Scan && root.getOutputInfo().hasProvenanceVariable(var))
        {
            return (Scan) root;
        }
        else
        {
            for (Operator child : root.getChildren())
            {
                // First traverse nested plans, then children = Depth First Search
                for (LogicalPlan plan : child.getLogicalPlansUsed())
                {
                    Operator nested_scan = getDefininingScanOperatorWithSubPlans(plan.getRootOperator(), var);
                    if (nested_scan != null)
                    {
                        return (Scan) nested_scan;
                    }
                }
                Operator child_scan = getDefininingScanOperatorWithSubPlans(child, var);
                if (child_scan != null)
                {
                    return (Scan) child_scan;
                }
                
            }
            for (LogicalPlan plan : root.getLogicalPlansUsed())
            {
                Operator nested_scan = getDefininingScanOperatorWithSubPlans(plan.getRootOperator(), var);
                if (nested_scan != null)
                {
                    return (Scan) nested_scan;
                }
            }
        }
        return null;
    }
    
    /**
     * Places an operator under the Product operator and on top of the operator that defines its relative variables.
     * 
     * @param op
     *            an operator.
     * @param product_op
     *            the Product operator of the logical plan.
     * @throws QueryCompilationException
     *             exception.
     */
    public static void placeOperatorUnderProduct(Operator op, Operator product_op) throws QueryCompilationException
    {
        // Place the operator on top of the operator that defines its relative variables
        Collection<RelativeVariable> relative_vars = LogicalPlanUtil.getRelativeVariables(op.getVariablesUsed());
        if (!relative_vars.isEmpty())
        {
            // Find the defining operator for the relative variable
            Operator lca = LogicalPlanUtil.getLowestCommonAncestor(product_op, relative_vars);
            if (lca == null)
            {
                product_op.addChild(op);
            }
            else
            {
                
                if (lca instanceof Navigate && op instanceof Scan)
                {
                    while (lca.getParent() instanceof Navigate
                            && getLowestCommonAncestor(lca.getParent(),
                                                       LogicalPlanUtil.getRelativeVariables(op.getVariablesUsed())).equals(lca))
                    {
                        lca = lca.getParent();
                    }
                }
                // Accommodate for self joins
                if (lca instanceof Scan && op instanceof Scan)
                {
                    assert (((Scan) lca).getTerm().equals(((Scan) op).getTerm()));
                    {
                        Ground ground = new Ground();
                        ground.updateOutputInfo();
                        op.addChild(ground);
                        
                        // Add as a child operator of the product
                        if (product_op != null)
                        {
                            product_op.addChild(op);
                        }
                    }
                }
                else
                {
                    // Add the operator on top of the chosen child
                    LogicalPlanUtil.insertOnTop(lca, op);
                }
            }
        }
        // Add the operator on top of the first child, which is initially a Ground operator
        else if (op instanceof Navigate)
        {
            LogicalPlanUtil.insertOnTop(product_op.getChildren().get(0), op);
        }
        // Add a ground child if the operator does not have a child and place it under the product
        else
        {
            if (op.getChildren().isEmpty())
            {
                Ground ground = new Ground();
                ground.updateOutputInfo();
                op.addChild(ground);
            }
            
            // Add as a child operator of the product
            product_op.addChild(op);
        }
        
        LogicalPlanUtil.updateAncestorOutputInfo(op);
    }
    
    /**
     * Inserts the fresh operator on top of the target operator. The target operator becomes a child operator of the fresh operator,
     * and the fresh operator becomes a child operator of the target operator's initial parent. It is assumed that the given fresh
     * operator is NOT part of a logical plan and has no parent. The method does NOT update the output info of any operator.
     * 
     * @param target
     *            an operator in a logical plan.
     * @param fresh
     *            an operator not in a logical plan.
     */
    public static void insertOnTop(Operator target, Operator fresh)
    {
        Operator parent = target.getParent();
        if (parent != null)
        {
            int index = parent.getChildren().indexOf(target);
            parent.removeChild(target);
            parent.addChild(index, fresh);
        }
        fresh.addChild(target);
    }
    
    /**
     * Inserts the fresh operator under of the target operator. The target operator becomes the parent of the fresh operator, and
     * the fresh operator becomes a child operator of the target operator's initial children. It is assumed that the given fresh
     * operator is NOT part of a logical plan and has no parent. The method does NOT update the output info of any operator.
     * 
     * @param target
     *            an operator in a logical plan.
     * @param fresh
     *            an operator not in a logical plan.
     */
    public static void insertUnder(Operator target, Operator fresh)
    {
        List<Operator> children = new ArrayList<Operator>(target.getChildren());
        for (Operator child : children)
        {
            target.removeChild(child);
            fresh.addChild(child);
        }
        target.addChild(fresh);
    }
    
    /**
     * Traverses a logical plan top-down and left-to-right and stacks the operators in order to visit them bottom-up and
     * right-to-left.
     * 
     * @param operator
     *            the root of a logical subtree.
     * @param stack
     *            the stack.
     */
    public static void stackBottomUpRightToLeft(Operator operator, Stack<Operator> stack)
    {
        stack.push(operator);
        
        for (Operator child : operator.getChildren())
        {
            stackBottomUpRightToLeft(child, stack);
        }
    }
    
    /**
     * Traverses a logical plan top-down and left-to-right and stacks the leaf operators in order to visit them bottom-up and
     * right-to-left.
     * 
     * @param operator
     *            the root of a logical subtree.
     * @param stack
     *            the stack.
     */
    public static void getAllLeafOperators(Operator operator, Stack<Operator> stack)
    {
        if (operator.getChildren().isEmpty())
        {
            stack.push(operator);
        }
        else
        {
            for (Operator child : operator.getChildren())
            {
                getAllLeafOperators(child, stack);
            }
        }
    }
    
    /**
     * Enumerates all the SendPlan operators in a plan.
     * 
     * @param plan
     *            the input plan
     * @return the list of SendPlan operators in the input plan
     */
    public static List<SendPlan> getAllSendPlans(LogicalPlan plan)
    {
        assert (plan != null);
        List<SendPlan> result = new ArrayList<SendPlan>();
        for (Assign assign : plan.getAssigns())
        {
            result.addAll(getAllSendPlans(assign.getPlan()));
        }
        result.addAll(getAllSendPlans(plan.getRootOperator()));
        return result;
    }
    
    /**
     * Enumerates all the send plan operators in a plan starting at a given root.
     * 
     * @param root
     *            the root of the input plan
     * @return the list of SendPlan operators in the input plan
     */
    private static List<SendPlan> getAllSendPlans(Operator root)
    {
        assert (root != null);
        List<SendPlan> result = new ArrayList<SendPlan>();
        for (Operator child : root.getChildren())
        {
            result.addAll(getAllSendPlans(child));
        }
        if (root instanceof SendPlan)
        {
            result.add((SendPlan) root);
        }
        for (LogicalPlan nested_plan : root.getLogicalPlansUsed())
        {
            result.addAll(getAllSendPlans(nested_plan));
        }
        return result;
    }
    
    /**
     * Put operators in a queue starting from the provided operator until the root in order to visit them top down and right to
     * left.
     * 
     * @param operator
     *            the root of a logical subtree.
     * @param queue
     *            the queue.
     */
    public static void queueTopDownRightToLeft(Operator operator, LinkedList<Operator> queue)
    {
        queue.add(operator);
        Operator parent = operator.getParent();
        if (parent == null) return;
        for (Operator child : parent.getChildren())
        {
            if (child != operator) queue.add(child);
        }
        queueTopDownRightToLeft(parent, queue);
    }
    
    /**
     * Merges terms into a conjunction.
     * 
     * @param terms
     *            the terms to merge.
     * @return an AndFunctionCall, or null if the input list is empty.
     */
    public static Term mergeTerms(List<Term> terms)
    {
        if (terms.isEmpty())
        {
            return null;
        }
        
        // Consolidate the join conditions into a single conjunctive condition
        List<Term> args = new ArrayList<Term>();
        
        Iterator<Term> iter = terms.iterator();
        Term and_func_call = iter.next();
        args.add(and_func_call);
        while (iter.hasNext())
        {
            args.add(iter.next());
            
            try
            {
                and_func_call = new GeneralFunctionCall(AndFunction.NAME, args);
            }
            catch (FunctionRegistryException e)
            {
                // This should never happen
                assert (false);
            }
            
            args.clear();
            args.add(and_func_call);
        }
        
        return and_func_call;
    }
    
    /**
     * Sets the execution data source of the input operator, if the data model of the data source is compatible with the operator.
     * 
     * @param operator
     *            a logical operator.
     * @param exec_data_source
     *            a data source name.
     * @param uas
     *            the unified application state.
     */
    public static void setExecutionDataSourceName(Operator operator, String exec_data_source, UnifiedApplicationState uas)
    {
        DataSource data_source = null;
        try
        {
            data_source = uas.getDataSource(exec_data_source);
        }
        catch (DataSourceException e)
        {
            assert (false);
        }
        
        if (operator.isDataSourceCompliant(data_source.getMetaData()))
        {
            operator.setExecutionDataSourceName(exec_data_source);
        }
    }
    
    public static boolean condition_is_Sql_Compliant(List<Term> conditions)
    {
        for (Term cond : conditions)
        {
            if (cond instanceof FunctionCall)
            {
                if (!((FunctionCall<?>) cond).getFunction().isSqlCompliant())
                {
                    return false;
                }
                else
                {
                    return condition_is_Sql_Compliant(((FunctionCall<?>) cond).getArguments());
                }
            }
        }
        return true;
    }
    
    /**
     * Adds conjunctively a condition to the input operator. If the input operator is a join operator, then the condition is added
     * to it and no new operators are created. Otherwise, the condition is added to a new or augments an existing select operator on
     * top of the input operator.
     * 
     * @param operator
     *            the operator.
     * @param condition
     *            the conjunctive condition.
     * @throws QueryCompilationException
     *             if there is a query compilation error.
     */
    public static void addCondition(Operator operator, Term condition) throws QueryCompilationException
    {
        if (operator instanceof InnerJoin)
        {
            ((InnerJoin) operator).addCondition(condition);
        }
        else if (operator instanceof Select)
        {
            ((Select) operator).addCondition(condition);
        }
        else
        {
            throw new AssertionError();
        }
    }
    
    public static void replaceParameterWithVariableInCondition(Term parent, Term condition,
            Map<Term, RelativeVariable> terms_to_vars)
    {
        // FIXME Need to handle also external, case, cast function calls?
        if (condition instanceof GeneralFunctionCall)
        {
            for (Term arg : ((GeneralFunctionCall) condition).getArguments())
            {
                replaceParameterWithVariableInCondition(condition, arg, terms_to_vars);
            }
        }
        
        else if (condition instanceof Parameter || condition instanceof Variable)
        {
            assert (parent instanceof GeneralFunctionCall);
            
            if (terms_to_vars.containsKey(condition))
            {
                RelativeVariable v = terms_to_vars.get(condition);
                // replace the parameter in the condition with the variable in the output info
                int index = ((GeneralFunctionCall) parent).removeArgument(condition);
                ((GeneralFunctionCall) parent).addArgument(index, v);
            }
        }
        
    }
    
    /**
     * Removes the input operator and replaces it with its single child, if the operator has a parent. The single child of the
     * operator is returned. The method does NOT update the output info of any operator.
     * 
     * @param operator
     *            an operator to remove.
     * @return the single child of the operator, or null if there is none.
     */
    public static Operator remove(Operator operator)
    {
        assert (operator.getChildren().size() <= 1);
        Operator only_child = null;
        if (operator.getChildren().size() == 1)
        {
            only_child = operator.getChildren().get(0);
        }
        Operator parent = operator.getParent();
        int index = -1;
        
        if (only_child != null)
        {
            operator.removeChild(only_child);
        }
        
        if (parent != null)
        {
            index = parent.getChildren().indexOf(operator);
            parent.removeChild(operator);
            parent.addChild(index, only_child);
        }
        
        return only_child;
    }
    
    /**
     * Replaces one operator with another. If the operator to remove is the root, then this is a no-op. The method does NOT update
     * the output info of any operator.
     * 
     * @param remove
     *            an operator to remove.
     * @param add
     *            an operator to add.
     */
    public static void replace(Operator remove, Operator add)
    {
        Operator parent = remove.getParent();
        if (parent == null) return;
        
        int index = -1;
        
        index = parent.getChildren().indexOf(remove);
        parent.removeChild(remove);
        parent.addChild(index, add);
    }
    
    /**
     * Replaces one operator with another. If the operator to remove is the root, then this is a no-op. The method does NOT update
     * the output info of any operator.
     * 
     * @param remove
     *            an operator to remove.
     * @param add
     *            an operator to add.
     */
    public static void replaceWithChildren(Operator remove, Operator add)
    {
        Operator parent = remove.getParent();
        if (parent == null) return;
        
        int index = -1;
        
        // Replacement for parent
        index = parent.getChildren().indexOf(remove);
        parent.removeChild(remove);
        parent.addChild(index, add);
        
        // Replacement for children
        List<Operator> children = new ArrayList<Operator>(remove.getChildren());
        for (Operator child : children)
        {
            remove.removeChild(child);
            add.addChild(child);
        }
    }
    
    /**
     * Returns the first operator in the input list that has all the input variables in its output info.
     * 
     * @param operators
     *            the operators to test.
     * @param variables
     *            the set of variables to find.
     * @return an operator.
     */
    public static Operator getFirstOperatorWithVariables(List<Operator> operators, Set<Variable> variables)
    {
        for (Operator op : operators)
        {
            if (op.getOutputInfo().getVariables().containsAll(variables)) return op;
        }
        
        throw new AssertionError();
    }
    
    /**
     * Recursively updates the output types of all operators in the logical query plan in a bottom-up fashion.
     * 
     * @param operator
     *            the current operator being visited.
     * @throws QueryCompilationException
     *             if there is a query compilation error.
     */
    public static void updateDescendentOutputInfo(Operator operator) throws QueryCompilationException
    {
        // Update children
        for (Operator child : operator.getChildren())
        {
            updateDescendentOutputInfo(child);
        }
        
        // Update nested plans
        for (LogicalPlan nested_plan : operator.getLogicalPlansUsed())
        {
            nested_plan.updateOutputInfoDeep();
        }
        
        // Update operator
        operator.updateOutputInfo();
    }
    
    /**
     * Recursively updates the output types of all ancestor operators of the input operator.
     * 
     * @param operator
     *            the current operator being visited.
     * @throws QueryCompilationException
     *             if there is a query compilation error.
     */
    public static void updateAncestorOutputInfo(Operator operator) throws QueryCompilationException
    {
        ((AbstractOperator) operator).updateOutputInfo();
        
        if (operator.getParent() != null)
        {
            updateAncestorOutputInfo(operator.getParent());
        }
    }
    
    /**
     * Creates a deep copy of the plan rooted at the given operator. The resulting tree has an up-to-date output info.
     * 
     * @param operator
     *            the current operator being visited.
     * @return a deep copy of the plan rooted at the given operator.
     */
    public static Operator copyDeep(Operator operator)
    {
        // Copy operator
        Operator copy = operator.copy();
        
        // Copy children
        for (Operator child : operator.getChildren())
        {
            copy.addChild(copyDeep(child));
        }
        
        // Update operator
        try
        {
            copy.updateOutputInfo();
        }
        catch (QueryCompilationException e)
        {
            assert (false);
        }
        
        return copy;
    }
    
    public static List<AbsoluteVariable> collectAbsoluteVariablesInAllSubPlans(LogicalPlan plan)
    {
        ArrayList<AbsoluteVariable> list = new ArrayList<AbsoluteVariable>();
        
        for (Operator op : plan.getRootOperator().getDescendantsAndSelf())
        {
            for (Variable var : op.getVariablesUsed())
            {
                if (var instanceof AbsoluteVariable) list.add((AbsoluteVariable) var);
            }
            
            for (LogicalPlan nested_plan : op.getLogicalPlansUsed())
            {
                list.addAll(collectAbsoluteVariablesInAllSubPlans(nested_plan));
            }
        }
        
        return list;
    }
    
    public static void updateAssignedTempSchemaObject(Assign assign) throws QueryExecutionException, QueryCompilationException
    {
        DataSource data_source = null;
        
        // Retrieve the schema object
        data_source = QueryProcessorFactory.getInstance().getDataSourceAccess(DataSource.TEMP_ASSIGN_SOURCE).getDataSource();
        // Remove the existing assigned temp schema object
        if (data_source.hasSchemaObject(assign.getTarget())) data_source.dropSchemaObject(assign.getTarget());
        SchemaTree schema_tree = new SchemaTree(assign.getPlan().getOutputType());
        data_source.createSchemaObject(assign.getTarget(), schema_tree, Size.SMALL);
        assign.updateOutputInfo();
    }
    
    /**
     * Given a query path valid above an operator, returns a query path corresponding to the same value below another given
     * operator. In other words, goes bottom-up through all the renamings of the variables in the query path and updates them. This
     * is an utility method for Jdbc implementation of DML operators. Romain: Not entirely clean, this should be removed when these
     * operators are refactored.
     * 
     * @param old_qp
     *            the query path valid above the start operator
     * @param start_op
     *            the start operator
     * @param end_op
     *            the end operator
     * @param stop_below_end_op
     *            if this flag is enabled, the returned query path is valid below the end operator. Otherwise, it is valid above the
     *            end operator.
     * @return the query path below the end operator pointing to the same value as the original query path, or <code>null</code> if
     *         the path could not be found.
     */
    public static QueryPath getQueryPathAboveRenamings(QueryPath old_qp, Operator start_op, Operator end_op,
            boolean stop_below_end_op)
    {
        Operator current_op = start_op.getParent();
        QueryPath current_qp = old_qp;
        RelativeVariable current_var = null;
        boolean success = false;
        while (current_op != null)
        {
            if (current_op == end_op)
            {
                success = true;
                if (stop_below_end_op)
                {
                    break;
                }
            }
            if (current_op instanceof Scan || current_op instanceof Exists || current_op instanceof Select
                    || current_op instanceof SendPlan || current_op instanceof ApplyPlan)
            {
                current_op = current_op.getParent();
            }
            else if (current_op instanceof Navigate)
            {
                Navigate nav = (Navigate) current_op;
                if (nav.getTerm().equals(current_qp))
                {
                    current_var = nav.getAliasVariable();
                }
                current_op = current_op.getParent();
            }
            else if (current_op instanceof Project)
            {
                Project project = (Project) current_op;
                QueryPath new_qp = null;
                for (Project.Item item : project.getProjectionItems())
                {
                    if (item.getTerm().equals(current_var) || item.getTerm().equals(current_qp))
                    {
                        new_qp = new QueryPath(project.getOutputInfo().getVariable(0), Arrays.asList(item.getAlias()));
                        break;
                    }
                }
                if (new_qp == null) return null;
                current_var = null;
                current_qp = new_qp;
                current_op = current_op.getParent();
            }
            else if (current_op instanceof Subquery)
            {
                Subquery subquery = (Subquery) current_op;
                if (current_qp == null) return null;
                current_qp = new QueryPath(subquery.getAliasVariable(), current_qp.getPathSteps());
                current_op = current_op.getParent();
            }
            else
            {
                // Other operators should not be encountered
                return null;
            }
        }
        
        if (success)
        {
            QueryPath final_qp = current_qp;
            if (final_qp == null && current_var != null)
            {
                final_qp = new QueryPath(current_var, Collections.<String> emptyList());
            }
            return final_qp;
        }
        else
        {
            // Couldn't find end_op in the the ancestors of start_op
            return null;
        }
    }
    
    /**
     * Given a relative variable or a query path valid above an operator, returns a query path corresponding to the same value above
     * another given operator. In other words, goes top-down through all the renamings of the variable and updates them. This is an
     * utility method for Jdbc implementation of DML operators. Romain: Not entirely clean, this should be removed when these
     * operators are refactored.
     * 
     * @param old_var
     *            the relative variable valid below the start operator
     * @param old_qp
     *            the query path valid above the start operator
     * @param start_op
     *            the start operator
     * @param end_op
     *            the end operator
     * @return the query path above the end operator pointing to the same value as the original query path, or <code>null</code> if
     *         the path could not be found.
     */
    private static QueryPath getQueryPathBelowRenamings(RelativeVariable old_var, QueryPath old_qp, Operator start_op,
            Operator end_op)
    {
        Operator current_op = start_op;
        QueryPath current_qp = old_qp;
        RelativeVariable current_var = old_var;
        while (current_op != null)
        {
            if (current_op == end_op)
            {
                QueryPath final_qp = current_qp;
                if (final_qp == null && current_var != null)
                {
                    final_qp = new QueryPath(current_var, Collections.<String> emptyList());
                }
                return final_qp;
            }
            if (current_op instanceof Scan || current_op instanceof Exists || current_op instanceof Select
                    || current_op instanceof SendPlan)
            {
                current_op = ((AbstractUnaryOperator) current_op).getChild();
            }
            else if (current_op instanceof Navigate)
            {
                Navigate nav = (Navigate) current_op;
                if (nav.getAliasVariable().equals(current_var))
                {
                    current_var = null;
                    Term nav_term = nav.getTerm();
                    if (nav_term instanceof QueryPath)
                    {
                        current_qp = (QueryPath) nav_term;
                    }
                    else
                    {
                        return null;
                    }
                }
                current_op = nav.getChild();
            }
            else if (current_op instanceof Project)
            {
                Project project = (Project) current_op;
                if (current_qp == null) return null;
                if (!(current_qp.getTerm() instanceof RelativeVariable)) return null;
                if (!((RelativeVariable) current_qp.getTerm()).getName().equals(Project.PROJECT_ALIAS)) return null;
                if (current_qp.getPathSteps().size() != 1) return null;
                for (Project.Item item : project.getProjectionItems())
                {
                    if (current_qp.getPathSteps().get(0).equals(item.getAlias()))
                    {
                        current_var = null;
                        current_qp = null;
                        
                        Term term = item.getTerm();
                        if (term instanceof RelativeVariable)
                        {
                            current_var = (RelativeVariable) term;
                        }
                        else if (term instanceof QueryPath)
                        {
                            current_qp = (QueryPath) term;
                        }
                        else
                        {
                            return null;
                        }
                        break;
                    }
                }
                current_op = project.getChild();
            }
            else if (current_op instanceof Subquery)
            {
                Subquery subquery = (Subquery) current_op;
                if (current_qp == null) return null;
                if (!(current_qp.getTerm() instanceof RelativeVariable)) return null;
                if (current_qp.getPathSteps().size() != 1) return null;
                
                Operator child = subquery.getChild();
                Term child_term = subquery.getOutputInfo().getProvenanceTerm((RelativeVariable) current_qp.getTerm());
                assert (child_term instanceof RelativeVariable);
                current_qp = new QueryPath(child_term, current_qp.getPathSteps());
                current_op = child;
            }
            else if (current_op instanceof Update)
            {
                current_op = ((Update) current_op).getChild();
            }
            else
            {
                // Other operators should not be encountered
                return null;
            }
        }
        
        // Couldn't find end_op in the the descendants of start_op
        return null;
    }
    
    /**
     * Given a relative variable valid above an operator, returns a query path corresponding to the same value above another given
     * operator. In other words, goes top-down through all the renamings of the variable and updates it. This is an utility method
     * for Jdbc implementation of DML operators. Romain: Not entirely clean, this should be removed when these operators are
     * refactored.
     * 
     * @param old_var
     *            the relative variable valid below the start operator
     * @param start_op
     *            the start operator
     * @param end_op
     *            the end operator
     * @return the query path above the end operator pointing to the same value as the original query path, or <code>null</code> if
     *         the path could not be found.
     */
    public static QueryPath getQueryPathBelowRenamings(RelativeVariable old_var, Operator start_op, Operator end_op)
    {
        return getQueryPathBelowRenamings(old_var, null, start_op, end_op);
    }
    
    /**
     * Given a query path valid above an operator, returns a query path corresponding to the same value above another given
     * operator. In other words, goes top-down through all the renamings of the variables and updates them. This is an utility
     * method for Jdbc implementation of DML operators. Romain: Not entirely clean, this should be removed when these operators are
     * refactored.
     * 
     * @param old_qp
     *            the query path valid above the start operator
     * @param start_op
     *            the start operator
     * @param end_op
     *            the end operator
     * @return the query path above the end operator pointing to the same value as the original query path, or <code>null</code> if
     *         the path could not be found.
     */
    public static QueryPath getQueryPathBelowRenamings(QueryPath old_qp, Operator start_op, Operator end_op)
    {
        return getQueryPathBelowRenamings(null, old_qp, start_op, end_op);
    }
    
    /**
     * Returns the number of operators in a logical plan. The result includes operators in nested plans.
     * 
     * @param plan
     *            the plan
     * @return the number of operators in the plan
     */
    public int countOperator(LogicalPlan plan)
    {
        return countOperator(plan.getRootOperator());
    }
    
    /**
     * Returns the number of operators in the subtree starting at the given root. The result includes operators in nested plans.
     * 
     * @param root
     *            the root of the subtree
     * @return the number of operators in the subtree
     */
    private int countOperator(Operator root)
    {
        int result = 1;
        for (LogicalPlan plan : root.getLogicalPlansUsed())
        {
            result += countOperator(plan.getRootOperator());
        }
        for (Operator child : root.getChildren())
        {
            result += countOperator(child);
        }
        return result;
    }
}
