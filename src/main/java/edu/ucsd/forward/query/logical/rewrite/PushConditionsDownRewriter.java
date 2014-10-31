package edu.ucsd.forward.query.logical.rewrite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.function.AbstractFunctionCall;
import edu.ucsd.forward.query.logical.ApplyPlan;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.logical.EliminateDuplicates;
import edu.ucsd.forward.query.logical.Exists;
import edu.ucsd.forward.query.logical.Ground;
import edu.ucsd.forward.query.logical.GroupBy;
import edu.ucsd.forward.query.logical.InnerJoin;
import edu.ucsd.forward.query.logical.Navigate;
import edu.ucsd.forward.query.logical.OffsetFetch;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.OuterJoin;
import edu.ucsd.forward.query.logical.PartitionBy;
import edu.ucsd.forward.query.logical.Product;
import edu.ucsd.forward.query.logical.Project;
import edu.ucsd.forward.query.logical.Project.ProjectQualifier;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.Select;
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
import edu.ucsd.forward.query.logical.term.Constant;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.TermUtil;
import edu.ucsd.forward.util.NameGenerator;
import edu.ucsd.forward.util.NameGeneratorFactory;

/**
 * The rewriter that pushes selections down. It will try to push down selections as deep as possible without introducing expensive
 * computation (e.g. duplicate a function evaluation in order to push a selection below a projection). It recursively tries to push
 * the conditions across all operators in a top-down manner. When it fails to do so, it goes back and puts the selection in the last
 * "valid place". Valid places are: above Scan and Subqueries, above GroupBy, inside join conditions.
 * 
 * @author Romain Vernoux
 */
public final class PushConditionsDownRewriter
{
    private UnifiedApplicationState m_uas;
    
    /**
     * Hidden constructor.
     * 
     * @param uas
     *            the unified application state.
     */
    private PushConditionsDownRewriter(UnifiedApplicationState uas)
    {
        assert (uas != null);
        m_uas = uas;
    }
    
    /**
     * Gets a new push-condition-down rewriter instance.
     * 
     * @param uas
     *            the unified application state.
     * @return a push-condition-down rewriter instance.
     */
    public static PushConditionsDownRewriter getInstance(UnifiedApplicationState uas)
    {
        return new PushConditionsDownRewriter(uas);
    }
    
    /**
     * Visits the input logical plan and pushes down the selections.
     * 
     * @param logical_plan
     *            the logical plan to visit
     * @return the logical plan with ordered join and product operators
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    public LogicalPlan rewrite(LogicalPlan logical_plan) throws QueryCompilationException
    {
        List<Term> conditions = new ArrayList<Term>();
        
        Operator root = logical_plan.getRootOperator();
        rewriteOperator(root, conditions);
        
        assert (conditions.isEmpty());
        
        logical_plan.updateOutputInfoDeep();
        
        // Remove the navigate that became dead because selections were pushed down
        for (Navigate nav : root.getDescendantsAndSelf(Navigate.class))
        {
            if (isDeadNavigate(nav))
            {
                LogicalPlanUtil.remove(nav);
            }
        }
        
        logical_plan.updateOutputInfoDeep();
        
        for (Assign assign : logical_plan.getAssigns())
        {
            rewriteAssign(assign, conditions);
            assert (conditions.isEmpty());
        }
        
        return logical_plan;
    }
    
    /**
     * Dispatches the call to specific methods based on the type of the operator to rewrite.
     * 
     * @param op
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteOperator(Operator op, List<Term> conditions) throws QueryCompilationException
    {
        if (op instanceof ApplyPlan) rewriteApplyPlan((ApplyPlan) op, conditions);
        else if (op instanceof EliminateDuplicates) rewriteEliminateDuplicates((EliminateDuplicates) op, conditions);
        else if (op instanceof Exists) rewriteExists((Exists) op, conditions);
        else if (op instanceof PartitionBy) rewritePartitionBy((PartitionBy) op, conditions);
        else if (op instanceof Ground) rewriteGround((Ground) op, conditions);
        else if (op instanceof OffsetFetch) rewriteOffsetFetch((OffsetFetch) op, conditions);
        else if (op instanceof Sort) rewriteSort((Sort) op, conditions);
        else if (op instanceof Select) rewriteSelect((Select) op, conditions);
        else if (op instanceof SetOperator) rewriteSetOperator((SetOperator) op, conditions);
        else if (op instanceof OuterJoin) rewriteOuterJoin((OuterJoin) op, conditions);
        else if (op instanceof InnerJoin) rewriteInnerJoin((InnerJoin) op, conditions);
        else if (op instanceof Product) rewriteProduct((Product) op, conditions);
        else if (op instanceof Project) rewriteProject((Project) op, conditions);
        else if (op instanceof Navigate) rewriteNavigate((Navigate) op, conditions);
        else if (op instanceof Scan) rewriteScan((Scan) op, conditions);
        else if (op instanceof GroupBy) rewriteGroupBy((GroupBy) op, conditions);
        else if (op instanceof Subquery) rewriteSubquery((Subquery) op, conditions);
        else if (op instanceof CreateDataObject) rewriteCreateSchemaObject((CreateDataObject) op, conditions);
        else if (op instanceof DropDataObject) rewriteDropSchemaObject((DropDataObject) op, conditions);
        else if (op instanceof Insert) rewriteInsert((Insert) op, conditions);
        else if (op instanceof Delete) rewriteDelete((Delete) op, conditions);
        else if (op instanceof Update) rewriteUpdate((Update) op, conditions);
        else
        {
            // These operator should not be in the plan at this point of the compilation pipeline.
            throw new UnsupportedOperationException();
        }
    }
    
    /**
     * Rewrites a CreateSchemaObject operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteCreateSchemaObject(CreateDataObject operator, List<Term> conditions) throws QueryCompilationException
    {
        assert (conditions.isEmpty());
        
        // Push the selections in the default plans
        for (SchemaPath path : operator.getTypesToSetDefault())
        {
            LogicalPlan plan = operator.getDefaultPlan(path);
            PushConditionsDownRewriter.getInstance(m_uas).rewrite(plan);
        }
    }
    
    /**
     * Rewrites a DropSchemaObject operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteDropSchemaObject(DropDataObject operator, List<Term> conditions) throws QueryCompilationException
    {
        assert (conditions.isEmpty());
    }
    
    /**
     * Rewrites an Insert operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteInsert(Insert operator, List<Term> conditions) throws QueryCompilationException
    {
        assert (conditions.isEmpty());
        
        // Push the selections in the insert plan
        PushConditionsDownRewriter.getInstance(m_uas).rewrite(operator.getLogicalPlansUsed().get(0));
    }
    
    /**
     * Rewrites a Delete operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteDelete(Delete operator, List<Term> conditions) throws QueryCompilationException
    {
        assert (conditions.isEmpty());
    }
    
    /**
     * Rewrites an Update operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteUpdate(Update operator, List<Term> conditions) throws QueryCompilationException
    {
        assert (conditions.isEmpty());
        
        // Push the selections in the assignment plans
        for (Assignment assignment : operator.getAssignments())
        {
            PushConditionsDownRewriter.getInstance(m_uas).rewrite(assignment.getLogicalPlan());
        }
    }
    
    /**
     * Rewrites an Assign operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteAssign(Assign operator, List<Term> conditions) throws QueryCompilationException
    {
        // Push the selections in the assign plan
        PushConditionsDownRewriter.getInstance(m_uas).rewrite(operator.getLogicalPlansUsed().get(0));
    }
    
    /**
     * Rewrites an ApplyPlan operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteApplyPlan(ApplyPlan operator, List<Term> conditions) throws QueryCompilationException
    {
        // Push the selections in the apply plan
        PushConditionsDownRewriter.getInstance(m_uas).rewrite(operator.getLogicalPlansUsed().get(0));
        
        // Get the conditions that cannot be pushed
        List<Term> non_pushable_conditions = getNonPushableConditions(conditions, Collections.singletonList(operator.getChild()));
        
        // Remove them from the list of conditions to push
        conditions.removeAll(non_pushable_conditions);
        
        // Try to push the pushable conditions
        rewriteOperator(operator.getChild(), conditions);
        
        // Get the pushable conditions that could not be pushed and add the non-pushable ones
        conditions.addAll(non_pushable_conditions);
    }
    
    /**
     * Rewrites an EliminateDuplicates operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteEliminateDuplicates(EliminateDuplicates operator, List<Term> conditions) throws QueryCompilationException
    {
        rewriteOperator(operator.getChild(), conditions);
    }
    
    /**
     * Rewrites an Exists operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteExists(Exists operator, List<Term> conditions) throws QueryCompilationException
    {
        // Push the selections in the exists plan
        PushConditionsDownRewriter.getInstance(m_uas).rewrite(operator.getLogicalPlansUsed().get(0));
        
        // Get the conditions that cannot be pushed
        List<Term> non_pushable_conditions = getNonPushableConditions(conditions, Collections.singletonList(operator.getChild()));
        
        // Remove them from the list of conditions to push
        conditions.removeAll(non_pushable_conditions);
        
        // Try to push the pushable conditions
        rewriteOperator(operator.getChild(), conditions);
        
        // Get the pushable conditions that could not be pushed and add the non-pushable ones
        conditions.addAll(non_pushable_conditions);
    }
    
    /**
     * Rewrites a PartitionBy operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewritePartitionBy(PartitionBy operator, List<Term> conditions) throws QueryCompilationException
    {
        // Get the conditions that cannot be pushed
        List<Term> non_pushable_conditions = getNonPushableConditions(conditions, Collections.singletonList(operator.getChild()));
        
        // Remove them from the list of conditions to push
        conditions.removeAll(non_pushable_conditions);
        
        // Try to push the pushable conditions
        rewriteOperator(operator.getChild(), conditions);
        
        // Get the pushable conditions that could not be pushed and add the non-pushable ones
        conditions.addAll(non_pushable_conditions);
    }
    
    /**
     * Rewrites a Ground operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteGround(Ground operator, List<Term> conditions) throws QueryCompilationException
    {
        // We cannot put selections here, let conditions as is so that they are placed somewhere above.
    }
    
    /**
     * Rewrites an OffsetFetch operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteOffsetFetch(OffsetFetch operator, List<Term> conditions) throws QueryCompilationException
    {
        rewriteOperator(operator.getChild(), conditions);
    }
    
    /**
     * Rewrites an Sort operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteSort(Sort operator, List<Term> conditions) throws QueryCompilationException
    {
        rewriteOperator(operator.getChild(), conditions);
    }
    
    /**
     * Rewrites an Select operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteSelect(Select operator, List<Term> conditions) throws QueryCompilationException
    {
        // Add all the conditions to the list of conditions
        List<Term> select_conditions = new ArrayList<Term>(operator.getConditions());
        for (Term select_condition : select_conditions)
        {
            conditions.add(select_condition);
            operator.removeCondition(select_condition);
        }
        
        // Tries to push the conditions down
        rewriteOperator(operator.getChild(), conditions);
        
        // Puts all the remaining conditions in this operator
        for (Term condition : conditions)
        {
            Term condition_to_add = condition;
            if (!select_conditions.contains(condition))
            {
                // Place Navigate below
                condition_to_add = placeNavigateBelowOperatorForTerm(condition, operator);
            }
            operator.addCondition(condition_to_add);
        }
        
        // Empties the list of conditions
        conditions.clear();
        
        operator.updateOutputInfo();
        
        // If there is no conditions in this operator, remove it
        if (operator.getConditions().size() == 0)
        {
            LogicalPlanUtil.remove(operator);
        }
        
    }
    
    /**
     * Rewrites a set operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteSetOperator(SetOperator operator, List<Term> conditions) throws QueryCompilationException
    {
        assert (operator.getChildren().size() == 2);
        
        // The list of conditions that could not be pushed to at least one of the children
        Set<Term> conditions_not_pushed = new HashSet<Term>();
        
        for (int i = 0; i < operator.getChildren().size(); i++)
        {
            Operator child = operator.getChildren().get(i);
            
            // Get the conditions that cannot be pushed
            List<Term> non_pushable_conditions = getNonPushableConditions(conditions, Collections.singletonList(child));
            
            // Add them to the list of conditions no pushed to a least one children
            conditions_not_pushed.addAll(non_pushable_conditions);
            
            // Create a copy of the conditions to push
            List<Term> conditions_copy = new ArrayList<Term>(conditions);
            
            // Remove them from the list of conditions to push
            conditions_copy.removeAll(non_pushable_conditions);
            
            // Try to push the pushable conditions
            rewriteOperator(child, conditions_copy);
            
            // Get the pushable conditions that could not be pushed and add the non-pushable ones
            conditions_not_pushed.addAll(conditions_copy);
        }
        
        conditions.clear();
        conditions.addAll(conditions_not_pushed);
    }
    
    /**
     * Rewrites an inner join operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteInnerJoin(InnerJoin operator, List<Term> conditions) throws QueryCompilationException
    {
        assert (operator.getChildren().size() == 2);
        
        // Add the inner join conditions to the list of conditions to push
        List<Term> inner_join_conditions = new ArrayList<Term>(operator.getConditions());
        for (Term inner_join_condition : inner_join_conditions)
        {
            conditions.add(inner_join_condition);
            operator.removeCondition(inner_join_condition);
        }
        
        for (int i = 0; i < operator.getChildren().size(); i++)
        {
            Operator child = operator.getChildren().get(i);
            
            // Get the conditions that cannot be pushed
            List<Term> non_pushable_conditions = getNonPushableConditions(conditions, Collections.singletonList(child));
            
            // Remove them from the list of conditions to push
            conditions.removeAll(non_pushable_conditions);
            
            // Try to push the pushable conditions
            rewriteOperator(child, conditions);
            
            // Get the pushable conditions that could not be pushed and add the non-pushable ones
            conditions.addAll(non_pushable_conditions);
        }
        
        operator.updateOutputInfo();
        
        // Add the non-pushed conditions to the inner join
        if (conditions.isEmpty())
        {
            operator.addCondition(new Constant(new BooleanValue(true)));
        }
        else
        {
            // Puts all the remaining conditions in this operator
            for (Term condition : conditions)
            {
                Term condition_to_add = condition;
                if (!inner_join_conditions.contains(condition))
                {
                    // Place Navigate below
                    condition_to_add = placeNavigateBelowOperatorForTerm(condition, operator);
                }
                operator.addCondition(condition_to_add);
            }
        }
        
        operator.updateOutputInfo();
        
        // All the conditions have been pushed
        conditions.clear();
    }
    
    /**
     * Rewrites an outer join operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteOuterJoin(OuterJoin operator, List<Term> conditions) throws QueryCompilationException
    {
        assert (operator.getChildren().size() == 2);
        
        // Save the conditions that come from above.
        List<Term> conditions_above = new ArrayList<Term>(conditions);
        conditions.clear();
        
        // The list of operator on which we can try to push down conditions
        List<Operator> pushable_children = new ArrayList<Operator>(1);
        
        switch (operator.getOuterJoinVariation())
        {
            case LEFT:
                // Push down only to the right child
                pushable_children.add(operator.getChildren().get(1));
                break;
            case RIGHT:
                // Push down only to the left child
                pushable_children.add(operator.getChildren().get(0));
                break;
            case FULL:
                // No conditions are pushed down
                break;
        }
        
        // Add the outer join conditions to the list of conditions to push
        List<Term> outer_join_conditions = new ArrayList<Term>(operator.getConditions());
        for (Term outer_join_condition : outer_join_conditions)
        {
            conditions.add(outer_join_condition);
            operator.removeCondition(outer_join_condition);
        }
        
        for (Operator child : pushable_children)
        {
            // Get the conditions that cannot be pushed
            List<Term> non_pushable_conditions = getNonPushableConditions(conditions, Collections.singletonList(child));
            
            // Remove them from the list of conditions to push
            conditions.removeAll(non_pushable_conditions);
            
            // Try to push the pushable conditions
            rewriteOperator(child, conditions);
            
            // Get the pushable conditions that could not be pushed and add the non-pushable ones
            conditions.addAll(non_pushable_conditions);
        }
        
        operator.updateOutputInfo();
        
        // Add the non-pushed conditions to the outer join
        if (conditions.isEmpty())
        {
            operator.addCondition(new Constant(new BooleanValue(true)));
        }
        else
        {
            // Puts all the remaining conditions in this operator
            for (Term condition : conditions)
            {
                Term condition_to_add = condition;
                if (!outer_join_conditions.contains(condition))
                {
                    // Place Navigate below
                    condition_to_add = placeNavigateBelowOperatorForTerm(condition, operator);
                }
                operator.addCondition(condition_to_add);
            }
        }
        
        operator.updateOutputInfo();
        
        // All the conditions have been pushed
        conditions.clear();
        
        // If there is no select operator above the OuterJoin, create one. Otherwise do nothing and pass the conditions back to the
        // selection.
        if (!conditions_above.isEmpty())
        {
            Operator parent = LogicalPlanUtil.getPreviousClauseOperator(operator);
            if (!(parent instanceof Select) && !(parent instanceof InnerJoin))
            {
                Select select = new Select();
                LogicalPlanUtil.insertOnTop(operator, select);
                
                // Puts all the conditions received from above
                for (Term condition : conditions_above)
                {
                    // Place Navigate below
                    Term rewritten_term = placeNavigateBelowOperatorForTerm(condition, select);
                    select.addCondition(rewritten_term);
                }
                
                select.updateOutputInfo();
            }
            else
            {
                conditions.addAll(conditions_above);
            }
        }
    }
    
    /**
     * Rewrites a product operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteProduct(Product operator, List<Term> conditions) throws QueryCompilationException
    {
        assert (operator.getChildren().size() > 1);
        
        for (int i = 0; i < operator.getChildren().size(); i++)
        {
            Operator child = operator.getChildren().get(i);
            
            // Get the conditions that cannot be pushed
            List<Term> non_pushable_conditions = getNonPushableConditions(conditions, Collections.singletonList(child));
            
            // Remove them from the list of conditions to push
            conditions.removeAll(non_pushable_conditions);
            
            // Try to push the pushable conditions
            rewriteOperator(child, conditions);
            
            // Get the pushable conditions that could not be pushed and add the non-pushable ones
            conditions.addAll(non_pushable_conditions);
        }
        
        // If some conditions remain, replace the product with joins
        if (!conditions.isEmpty())
        {
            List<Operator> product_children = new ArrayList<Operator>(operator.getChildren());
            Operator left_child = product_children.get(0);
            operator.removeChild(left_child);
            
            for (int i = 1; i < product_children.size(); i++)
            {
                InnerJoin join = new InnerJoin();
                join.addChild(left_child);
                
                Operator right_child = product_children.get(i);
                operator.removeChild(right_child);
                join.addChild(right_child);
                
                // Get the conditions that cannot be pushed to these two operators and push the other ones
                List<Term> non_pushable_conditions = getNonPushableConditions(conditions, Arrays.asList(left_child, right_child));
                conditions.removeAll(non_pushable_conditions);
                if (conditions.isEmpty())
                {
                    join.addCondition(new Constant(new BooleanValue(true)));
                }
                else
                {
                    for (Term condition : conditions)
                    {
                        // Place Navigate below
                        Term rewritten_term = placeNavigateBelowOperatorForTerm(condition, join);
                        join.addCondition(rewritten_term);
                    }
                }
                conditions.clear();
                conditions.addAll(non_pushable_conditions);
                
                join.updateOutputInfo();
                
                left_child = join;
            }
            
            // Replace operators
            LogicalPlanUtil.replace(operator, left_child);
            left_child.updateOutputInfo();
            
            // All the conditions have been pushed
            conditions.clear();
        }
    }
    
    /**
     * Rewrites a project operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteProject(Project operator, List<Term> conditions) throws QueryCompilationException
    {
        if (conditions.isEmpty())
        {
            rewriteOperator(operator.getChild(), conditions);
            
            assert (conditions.isEmpty());
        }
        else
        {
            // Get the list of renamings from the project operator
            Map<Term, Term> substitutions = new HashMap<Term, Term>();
            if (operator.getProjectQualifier() == ProjectQualifier.TUPLE)
            {
                for (Project.Item item : operator.getProjectionItems())
                {
                    Term term = item.getTerm();
                    if (!(term instanceof AbstractFunctionCall<?>))
                    {
                        RelativeVariable project_var = (RelativeVariable) operator.getOutputInfo().getVariable(0);
                        assert (project_var.getName().equals(Project.PROJECT_ALIAS));
                        QueryPath qp = new QueryPath(project_var, Arrays.asList(item.getAlias()));
                        qp.inferType(Collections.<Operator> emptyList());
                        substitutions.put(qp, term);
                    }
                }
            }
            else
            {
                assert (operator.getProjectionItems().size() == 1);
                Project.Item item = operator.getProjectionItems().get(0);
                Term term = item.getTerm();
                if (!(term instanceof AbstractFunctionCall<?>))
                {
                    RelativeVariable project_var = (RelativeVariable) operator.getOutputInfo().getVariable(0);
                    assert (project_var.getName().equals(Project.PROJECT_ALIAS));
                    substitutions.put(project_var, term);
                }
            }
            
            // Rewrite the list of conditions to reverse the renamings and save a correspondance map between original condition and
            // rewritten condition.
            Map<Term, Term> conditions_rewritings = new HashMap<Term, Term>();
            List<Term> rewritten_conditions = new ArrayList<Term>();
            for (Term condition : conditions)
            {
                Term rewritten_condition = TermUtil.substitute(condition, substitutions);
                conditions_rewritings.put(rewritten_condition, condition);
                rewritten_conditions.add(rewritten_condition);
            }
            
            conditions.clear();
            
            // Get the conditions that cannot be pushed
            List<Term> non_pushable_conditions_rewritten = getNonPushableConditions(rewritten_conditions,
                                                                                    Collections.singletonList(operator.getChild()));
            List<Term> non_pushable_conditions = new ArrayList<Term>();
            for (Term condition_rewritten : non_pushable_conditions_rewritten)
            {
                Term condition = conditions_rewritings.get(condition_rewritten);
                assert condition != null;
                non_pushable_conditions.add(condition);
            }
            
            // Remove them from the list of conditions to push
            rewritten_conditions.removeAll(non_pushable_conditions_rewritten);
            
            // Try to push the pushable conditions
            rewriteOperator(operator.getChild(), rewritten_conditions);
            
            // Propagate the conditions that could not be pushed
            conditions.addAll(non_pushable_conditions);
            for (Term condition_rewritten : rewritten_conditions)
            {
                Term condition = conditions_rewritings.get(condition_rewritten);
                assert condition != null;
                conditions.add(condition);
            }
        }
    }
    
    /**
     * Rewrites a Navigate operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteNavigate(Navigate operator, List<Term> conditions) throws QueryCompilationException
    {
        if (conditions.isEmpty())
        {
            rewriteOperator(operator.getChild(), conditions);
            
            assert (conditions.isEmpty());
        }
        else
        {
            // Create the substitution map
            Map<Term, Term> substitutions = new HashMap<Term, Term>();
            substitutions.put(operator.getAliasVariable(), operator.getTerm());
            
            // Rewrite the list of conditions to inline the navigation term and save a correspondence map between original condition
            // and rewritten condition.
            Map<Term, Term> conditions_rewritings = new HashMap<Term, Term>();
            List<Term> rewritten_conditions = new ArrayList<Term>();
            for (Term condition : conditions)
            {
                Term rewritten_condition = TermUtil.substitute(condition, substitutions);
                conditions_rewritings.put(rewritten_condition, condition);
                rewritten_conditions.add(rewritten_condition);
            }
            
            conditions.clear();
            
            // Try to push the conditions
            rewriteOperator(operator.getChild(), rewritten_conditions);
            
            // Propagate the conditions that could not be pushed
            for (Term condition_rewritten : rewritten_conditions)
            {
                Term condition = conditions_rewritings.get(condition_rewritten);
                assert condition != null;
                conditions.add(condition);
            }
        }
    }
    
    /**
     * Rewrites a Scan operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteScan(Scan operator, List<Term> conditions) throws QueryCompilationException
    {
        // If the scan corresponds to a FLATTEN, push the conditions in the child
        if (!(operator.getChild() instanceof Ground))
        {
            // Get the conditions that cannot be pushed
            List<Term> non_pushable_conditions = getNonPushableConditions(conditions,
                                                                          Collections.singletonList(operator.getChild()));
            
            // Remove them from the list of conditions to push
            conditions.removeAll(non_pushable_conditions);
            
            // Try to push the pushable conditions
            rewriteOperator(operator.getChild(), conditions);
            
            // Get the pushable conditions that could not be pushed and add the non-pushable ones
            conditions.addAll(non_pushable_conditions);
        }
        
        // If there is no select operator above the scan, create one. Otherwise do nothing and pass the conditions back to the
        // selection.
        if (!(LogicalPlanUtil.getPreviousClauseOperator(operator) instanceof Select) && !conditions.isEmpty())
        {
            Select select = new Select();
            LogicalPlanUtil.insertOnTop(operator, select);
            
            // Puts all the remaining conditions in this operator
            for (Term condition : conditions)
            {
                // Place Navigate below
                Term rewritten_term = placeNavigateBelowOperatorForTerm(condition, select);
                select.addCondition(rewritten_term);
            }
            
            select.updateOutputInfo();
            
            // All the conditions have been pushed
            conditions.clear();
        }
    }
    
    /**
     * Rewrites a Subquery operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteSubquery(Subquery operator, List<Term> conditions) throws QueryCompilationException
    {
        
        if (conditions.isEmpty())
        {
            rewriteOperator(operator.getChild(), conditions);
            
            assert (conditions.isEmpty());
        }
        else
        {
            // Create the substitution map
            Map<Term, Term> substitutions = new HashMap<Term, Term>();
            RelativeVariable project_var = (RelativeVariable) operator.getChild().getOutputInfo().getVariable(0);
            assert (project_var.getName().equals(Project.PROJECT_ALIAS));
            substitutions.put(operator.getAliasVariable(), project_var);
            
            // Rewrite the list of conditions to reverse the renaming and save a correspondance map between original condition and
            // rewritten condition.
            Map<Term, Term> conditions_rewritings = new HashMap<Term, Term>();
            List<Term> rewritten_conditions = new ArrayList<Term>();
            for (Term condition : conditions)
            {
                Term rewritten_condition = TermUtil.substitute(condition, substitutions);
                conditions_rewritings.put(rewritten_condition, condition);
                rewritten_conditions.add(rewritten_condition);
            }
            
            conditions.clear();
            
            // Get the conditions that cannot be pushed (e.g. those which use the position variable)
            List<Term> non_pushable_conditions_rewritten = getNonPushableConditions(rewritten_conditions,
                                                                                    Collections.singletonList(operator.getChild()));
            List<Term> non_pushable_conditions = new ArrayList<Term>();
            for (Term condition_rewritten : non_pushable_conditions_rewritten)
            {
                Term condition = conditions_rewritings.get(condition_rewritten);
                assert condition != null;
                non_pushable_conditions.add(condition);
            }
            
            // Remove them from the list of conditions to push
            rewritten_conditions.removeAll(non_pushable_conditions_rewritten);
            
            // Try to push the pushable conditions
            rewriteOperator(operator.getChild(), rewritten_conditions);
            
            // Get the conditions that could not be pushed
            conditions.addAll(non_pushable_conditions);
            for (Term condition_rewritten : rewritten_conditions)
            {
                Term condition = conditions_rewritings.get(condition_rewritten);
                assert condition != null;
                conditions.add(condition);
            }
            
            // If there is no select operator above the subquery, create one. Otherwise do nothing and pass the conditions back to
            // the selection.
            if (!(LogicalPlanUtil.getPreviousClauseOperator(operator) instanceof Select) && !conditions.isEmpty())
            {
                Select select = new Select();
                LogicalPlanUtil.insertOnTop(operator, select);
                
                // Puts all the remaining conditions in this operator
                for (Term condition : conditions)
                {
                    // Place Navigate below
                    Term rewritten_term = placeNavigateBelowOperatorForTerm(condition, select);
                    select.addCondition(rewritten_term);
                }
                
                select.updateOutputInfo();
                
                conditions.clear();
            }
        }
    }
    
    /**
     * Rewrites a GroupBy operator.
     * 
     * @param operator
     *            the operator to rewrite
     * @param conditions
     *            the list of conditions to push
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private void rewriteGroupBy(GroupBy operator, List<Term> conditions) throws QueryCompilationException
    {
        if (conditions.isEmpty())
        {
            rewriteOperator(operator.getChild(), conditions);
            
            assert (conditions.isEmpty());
        }
        else
        {
            // Get the list of renamings from the group by operator
            Map<Term, Term> substitutions = new HashMap<Term, Term>();
            for (GroupBy.Item item : operator.getGroupByItems())
            {
                Term term = item.getTerm();
                RelativeVariable var = item.getVariable();
                if (!(term instanceof AbstractFunctionCall<?>))
                {
                    substitutions.put(var, term);
                }
            }
            
            // Rewrite the list of conditions to reverse the renamings and save a correspondance map between original condition and
            // rewritten condition.
            Map<Term, Term> conditions_rewritings = new HashMap<Term, Term>();
            List<Term> rewritten_conditions = new ArrayList<Term>();
            for (Term condition : conditions)
            {
                Term rewritten_condition = TermUtil.substitute(condition, substitutions);
                conditions_rewritings.put(rewritten_condition, condition);
                rewritten_conditions.add(rewritten_condition);
            }
            
            conditions.clear();
            
            // Get the conditions that cannot be pushed
            List<Term> non_pushable_conditions_rewritten = getNonPushableConditions(rewritten_conditions,
                                                                                    Collections.singletonList(operator.getChild()));
            List<Term> non_pushable_conditions = new ArrayList<Term>();
            for (Term condition_rewritten : non_pushable_conditions_rewritten)
            {
                Term condition = conditions_rewritings.get(condition_rewritten);
                assert condition != null;
                non_pushable_conditions.add(condition);
            }
            
            // Remove them from the list of conditions to push
            rewritten_conditions.removeAll(non_pushable_conditions_rewritten);
            
            // Try to push the pushable conditions
            rewriteOperator(operator.getChild(), rewritten_conditions);
            
            // Propagate the conditions that could not be pushed
            conditions.addAll(non_pushable_conditions);
            for (Term condition_rewritten : rewritten_conditions)
            {
                Term condition = conditions_rewritings.get(condition_rewritten);
                assert condition != null;
                conditions.add(condition);
            }
            
            // If there is no select operator above the groupby, create one. Otherwise do nothing and pass the conditions back to
            // the selection.
            if (!(LogicalPlanUtil.getPreviousClauseOperator(operator) instanceof Select) && !conditions.isEmpty())
            {
                Select select = new Select();
                LogicalPlanUtil.insertOnTop(operator, select);
                
                // Puts all the remaining conditions in this operator
                for (Term condition : conditions)
                {
                    // Place Navigate below
                    Term rewritten_term = placeNavigateBelowOperatorForTerm(condition, select);
                    select.addCondition(rewritten_term);
                }
                
                select.updateOutputInfo();
                
                conditions.clear();
            }
        }
    }
    
    /**
     * Looks into a list of conditions and returns a list of those that cannot be placed above the given operators, because these
     * operators do not output all the variables used by the condition.
     * 
     * @param conditions
     *            the conditions to push
     * @param ops
     *            the operators on top of which we want to place the conditions
     * @return the list of variables that cannot be placed on top of the operator
     */
    private List<Term> getNonPushableConditions(List<Term> conditions, List<Operator> ops)
    {
        List<Term> result = new ArrayList<Term>();
        List<RelativeVariable> outputed_vars = new ArrayList<RelativeVariable>();
        for (Operator op : ops)
        {
            outputed_vars.addAll(op.getOutputInfo().getVariables());
        }
        for (Term condition : conditions)
        {
            List<RelativeVariable> rel_vars = LogicalPlanUtil.getRelativeVariables(condition.getVariablesUsed());
            if (!outputed_vars.containsAll(rel_vars))
            {
                result.add(condition);
            }
        }
        return result;
    }
    
    /**
     * Places the Navigate operators required by a term below the operators that contains this term. Returns a rewriting of the
     * original term in which the query paths are replaced by the variables provided by the Navigate operators.
     * 
     * @param term
     *            the term requiring Navigate operators
     * @param op
     *            the operator in which the term is located
     * @return the rewriting of the original term in which the query paths are replaced by the variables provided by the Navigate
     *         operators.
     * @throws QueryCompilationException
     *             if an error occurs during the rewriting
     */
    private Term placeNavigateBelowOperatorForTerm(Term term, Operator op) throws QueryCompilationException
    {
        Term term_copy = term;
        
        // Get all the query paths used by the term
        List<QueryPath> used_qp = getQueryPathsUsed(term_copy);
        
        // For each of them, insert Navigate operator
        for (QueryPath qp : used_qp)
        {
            if (qp.getTerm() instanceof RelativeVariable)
            {
                RelativeVariable rel_var = (RelativeVariable) qp.getTerm();
                
                // Find the child on top of which to put the Navigate
                Operator child = null;
                for (Operator op_child : op.getChildren())
                {
                    if (op_child.getOutputInfo().getVariables().contains(rel_var))
                    {
                        child = op_child;
                        break;
                    }
                }
                assert (child != null);
                
                // Add the Navigate operator
                String fresh_alias = NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.PUSH_DOWN_SELECTIONS_GENERATOR);
                Navigate nav = new Navigate(fresh_alias, qp);
                LogicalPlanUtil.insertOnTop(child, nav);
                nav.updateOutputInfo();
                
                // Rewrite the term to replace the query path with the variable from the Navigate
                Map<Term, Term> substitutions = new HashMap<Term, Term>();
                substitutions.put(qp, nav.getAliasVariable());
                term_copy = TermUtil.substitute(term_copy, substitutions);
            }
        }
        
        return term_copy;
    }
    
    /**
     * Returns all the query paths used by a term.
     * 
     * @param term
     *            the term
     * @return the list of query paths used by the term
     */
    private List<QueryPath> getQueryPathsUsed(Term term)
    {
        if (term instanceof QueryPath)
        {
            assert !(((QueryPath) term).getTerm() instanceof QueryPath);
            return Arrays.asList((QueryPath) term);
        }
        else if (term instanceof AbstractFunctionCall<?>)
        {
            List<QueryPath> result = new ArrayList<QueryPath>();
            for (Term sub_term : ((AbstractFunctionCall<?>) term).getArguments())
            {
                result.addAll(getQueryPathsUsed(sub_term));
            }
            return result;
        }
        else
        {
            // Constant, RelativeVariable, AbsoluteVariable or Parameter
            return Collections.<QueryPath> emptyList();
        }
    }
    
    /**
     * Tells if a Navigate operator became dead as a result of pushing selections down. A Navigate is called dead if the result of
     * its navigation is never used.
     * 
     * @param operator
     *            the operator to test
     * @return <code>true</code> if the given operator is dead, <code>false</code> otherwise.
     */
    private boolean isDeadNavigate(Navigate operator)
    {
        Operator op = operator;
        do
        {
            op = op.getParent();
            if (op == null) break;
            if (op.getVariablesUsed().contains(operator.getAliasVariable()))
            {
                return false;
            }
        } while (!(op instanceof Project || op instanceof GroupBy));
        
        return true;
    }
}
