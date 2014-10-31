package edu.ucsd.forward.query.logical.visitors;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.ast.SetOpExpression;
import edu.ucsd.forward.query.ast.SetOpExpression.SetOpType;
import edu.ucsd.forward.query.logical.AntiSemiJoin;
import edu.ucsd.forward.query.logical.ApplyPlan;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;
import edu.ucsd.forward.query.logical.Copy;
import edu.ucsd.forward.query.logical.EliminateDuplicates;
import edu.ucsd.forward.query.logical.Exists;
import edu.ucsd.forward.query.logical.Ground;
import edu.ucsd.forward.query.logical.GroupBy;
import edu.ucsd.forward.query.logical.IndexScan;
import edu.ucsd.forward.query.logical.InnerJoin;
import edu.ucsd.forward.query.logical.Navigate;
import edu.ucsd.forward.query.logical.OffsetFetch;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.OuterJoin;
import edu.ucsd.forward.query.logical.Subquery;
import edu.ucsd.forward.query.logical.OuterJoin.Variation;
import edu.ucsd.forward.query.logical.PartitionBy;
import edu.ucsd.forward.query.logical.Product;
import edu.ucsd.forward.query.logical.Project;
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
import edu.ucsd.forward.query.logical.plan.LogicalPlanUtil;
import edu.ucsd.forward.query.logical.term.AbsoluteVariable;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;

/**
 * The operator visitor that determines the order of join and product operators, and sets the cardinality estimates of all
 * operators.
 * 
 * @author <Vicky Papavasileiou>
 * @author Michalis Petropoulos
 * 
 */
public final class JoinOrderRewriter extends AbstractOperatorVisitor
{
    private UnifiedApplicationState m_uas;
    
    /**
     * Hidden constructor.
     * 
     * @param uas
     *            the unified application state.
     */
    private JoinOrderRewriter(UnifiedApplicationState uas)
    {
        assert (uas != null);
        m_uas = uas;
    }
    
    /**
     * Gets a new join order rewriter instance.
     * 
     * @param uas
     *            the unified application state.
     * @return a join order rewriter instance.
     */
    public static JoinOrderRewriter getInstance(UnifiedApplicationState uas)
    {
        return new JoinOrderRewriter(uas);
    }
    
    /**
     * Visits the input logical plan, orders the join and product operators, and sets the cardinality estimates of all operators.
     * 
     * @param plan
     *            the logical plan to visit.
     * @return the logical plan with ordered join and product operators.
     */
    public LogicalPlan rewrite(LogicalPlan plan)
    {
        Stack<Operator> stack = new Stack<Operator>();
        LogicalPlanUtil.stackBottomUpRightToLeft(plan.getRootOperator(), stack);
        Operator new_root = null;
        while (!stack.isEmpty())
        {
            new_root = stack.pop().accept(this);
        }
        
        // New root
        Operator old_root = plan.getRootOperator();
        if (old_root != new_root)
        {
            plan.setRootOperator(new_root);
            plan.updateOutputInfoShallow();
        }
        
        // Rewrite assign
        for (Assign assign : plan.getAssigns())
        {
            JoinOrderRewriter.getInstance(m_uas).rewrite(assign.getLogicalPlansUsed().get(0));
        }
        return plan;
    }
    
    @Override
    public Operator visitAntiSemiJoin(AntiSemiJoin operator)
    {
        operator.setCardinalityEstimate(estimateJoin(operator));
        
        return operator;
    }
    
    @Override
    public Operator visitApplyPlan(ApplyPlan operator)
    {
        // Order the joins of the apply plan
        JoinOrderRewriter.getInstance(m_uas).rewrite(operator.getLogicalPlansUsed().get(0));
        
        // Copy the cardinality estimate of the child operator
        operator.setCardinalityEstimate(operator.getChild().getCardinalityEstimate());
        
        return operator;
    }
    
    @Override
    public Operator visitCopy(Copy operator)
    {
        // Order the joins of the copy plan
        JoinOrderRewriter.getInstance(m_uas).rewrite(operator.getCopyPlan());
        
        // Copy the cardinality estimate of the copy plan
        operator.setCardinalityEstimate(operator.getCopyPlan().getRootOperator().getCardinalityEstimate());
        
        return operator;
    }
    
    @Override
    public Operator visitEliminateDuplicates(EliminateDuplicates operator)
    {
        // Copy the cardinality estimate of the child operator
        operator.setCardinalityEstimate(operator.getChild().getCardinalityEstimate());
        
        return operator;
    }
    
    @Override
    public Operator visitExists(Exists operator)
    {
        // Order the joins of the apply plan
        JoinOrderRewriter.getInstance(m_uas).rewrite(operator.getLogicalPlansUsed().get(0));
        
        // Copy the cardinality estimate of the child operator
        operator.setCardinalityEstimate(operator.getChild().getCardinalityEstimate());
        
        return operator;
    }
    
    @Override
    public Operator visitGround(Ground operator)
    {
        operator.setCardinalityEstimate(Size.ONE);
        
        return operator;
    }
    
    @Override
    public Operator visitGroupBy(GroupBy operator)
    {
        operator.setCardinalityEstimate(Size.SMALL);
        
        return operator;
    }
    
    @Override
    public Operator visitInnerJoin(InnerJoin operator)
    {
        Operator result = orderByIngres(operator);
        
        return result;
    }
    
    @Override
    public Operator visitNavigate(Navigate operator)
    {
        // Copy the cardinality estimate of the child operator
        operator.setCardinalityEstimate(operator.getChild().getCardinalityEstimate());
        
        return operator;
    }
    
    @Override
    public Operator visitOffsetFetch(OffsetFetch operator)
    {
        // FIXME Establish threshold between SMALL and LARGE and compare it with FETCH value to estimate cardinality
        // Copy the cardinality estimate of the child operator
        operator.setCardinalityEstimate(operator.getChild().getCardinalityEstimate());
        
        return operator;
    }
    
    @Override
    public Operator visitOuterJoin(OuterJoin operator)
    {
        operator.setCardinalityEstimate(estimateJoin(operator));
        
        return operator;
    }
    
    @Override
    public Operator visitPartitionBy(PartitionBy operator)
    {
        // Copy the cardinality estimate of the child operator
        operator.setCardinalityEstimate(operator.getChild().getCardinalityEstimate());
        
        return operator;
    }
    
    @Override
    public Operator visitProduct(Product operator)
    {
        Operator result = orderByIngres(operator);
        
        return result;
    }
    
    @Override
    public Operator visitProject(Project operator)
    {
        // Copy the cardinality estimate of the child operator
        operator.setCardinalityEstimate(operator.getChild().getCardinalityEstimate());
        
        return operator;
    }
    
    @Override
    public Operator visitScan(Scan operator)
    {
        Size size = Size.UNKNOWN;
        
        for (AbsoluteVariable var : LogicalPlanUtil.getAbsoluteVariables(operator.getVariablesUsed()))
        {
            try
            {
                Size do_size = m_uas.getDataSource(var.getDataSourceName()).getSchemaObject(var.getSchemaObjectName()).getCardinalityEstimate();
                if (do_size != Size.UNKNOWN && size != Size.LARGE)
                {
                    size = do_size;
                }
            }
            catch (DataSourceException e)
            {
                assert (false);
            }
            catch (QueryExecutionException e)
            {
                assert (false);
            }
        }
        
        Size child_size = operator.getChild().getCardinalityEstimate();
        if (size == Size.UNKNOWN)
        {
            size = child_size;
        }
        
        operator.setCardinalityEstimate(size);
        
        return operator;
    }
    
    @Override
    public Operator visitSelect(Select operator)
    {
        operator.setCardinalityEstimate(Size.SMALL);
        
        return operator;
    }
    
    @Override
    public Operator visitSemiJoin(SemiJoin operator)
    {
        operator.setCardinalityEstimate(estimateJoin(operator));
        
        return operator;
    }
    
    @Override
    public Operator visitSendPlan(SendPlan operator)
    {
        // Order the joins of the send plan
        JoinOrderRewriter.getInstance(m_uas).rewrite(operator.getSendPlan());
        
        // Copy the cardinality estimate of the send plan
        operator.setCardinalityEstimate(operator.getSendPlan().getRootOperator().getCardinalityEstimate());
        
        return operator;
    }
    
    @Override
    public Operator visitSetOperator(SetOperator operator)
    {
        if (operator.getSetOpType() == SetOpType.OUTER_UNION)
        {
            List<Operator> children = operator.getChildren();
            Size size = null;
            assert (operator.getChildren().size() == 2);
            if (children.get(0).getCardinalityEstimate().equals(Size.LARGE)
                    || children.get(1).getCardinalityEstimate().equals(Size.LARGE)) size = Size.LARGE;
            else size = Size.SMALL;
            operator.setCardinalityEstimate(size);
            return operator;
        }
        
        operator.setCardinalityEstimate(estimateSetOp(operator));
        
        return operator;
    }
    
    @Override
    public Operator visitSort(Sort operator)
    {
        // Copy the cardinality estimate of the child operator
        operator.setCardinalityEstimate(operator.getChild().getCardinalityEstimate());
        
        return operator;
    }
    
    /**
     * Orders join and product operators according to the INGRES algorithm. Takes as input an N-ary Join and produces a tree of
     * binary joins and products. Products are created when the variables appearing in a condition are from more than two tables.
     * 
     * @param join
     *            the join or product operator to order.
     * @return a left-deep join tree.
     */
    private Operator orderByIngres(Operator join)
    {
        Operator left = null;
        
        List<Operator> children = join.getChildren();
        
        // Pick smallest sized child
        for (Operator op : children)
        {
            if (left == null || op.getCardinalityEstimate() == Size.SMALL && left.getCardinalityEstimate() != Size.ONE)
            {
                left = op;
            }
            else if (op.getCardinalityEstimate() == Size.ONE)
            {
                left = op;
                break;
            }
        }
        
        // If there isn't any small child, pick any
        if (left == null) left = children.get(0);
        
        while (join.getChildren().size() > 1)
        {
            // The variables of the left child
            Set<RelativeVariable> left_vars = left.getOutputInfo().getVariables();
            InnerJoin binary_join = null;
            Operator right = null;
            if (join instanceof InnerJoin)
            {
                
                List<Term> conditions = new ArrayList<Term>(((InnerJoin) join).getConditions());
                
                // Find the condition that the variables of the left child appear in
                for (Term cond : conditions)
                {
                    Set<Variable> cond_vars = new HashSet<Variable>(LogicalPlanUtil.getRelativeVariables(cond.getVariablesUsed()));
                    // If small child variables appears in condition
                    
                    if (cond_vars.removeAll(left_vars))
                    {
                        // If cond_vars is empty, PushConditionsDown should have pushed further down as a Select
                        assert (!cond_vars.isEmpty());
                        
                        // Find the next child operator that can join with the small one
                        // We want to find the smallest in size, if it exists
                        Operator min = null;
                        for (Operator child : join.getChildren())
                        {
                            if (child.getOutputInfo().getVariables().containsAll(cond_vars))
                            {
                                if (child.getCardinalityEstimate().equals(Size.ONE))
                                {
                                    min = child;
                                    break;
                                }
                                else if (child.getCardinalityEstimate().equals(Size.SMALL))
                                {
                                    if (min == null) min = child;
                                    else
                                    {
                                        if (min.getCardinalityEstimate().equals(Size.LARGE)) min = child;
                                    }
                                }
                                else if (child.getCardinalityEstimate().equals(Size.LARGE))
                                {
                                    if (min == null) min = child;
                                }
                            }
                        }
                        right = min; // FIXME right can be null because no join was found
                        
                        if (right == null)
                        {
                            break; // It is a product
                        }
                        
                        join.removeChild(left);
                        join.removeChild(right);
                        ((InnerJoin) join).removeCondition(cond);
                        binary_join = new InnerJoin();
                        binary_join.addChild(left);
                        binary_join.addChild(right);
                        binary_join.addCondition(cond);
                        binary_join.setCardinalityEstimate(estimateJoin(binary_join));
                        join.addChild(binary_join);
                        
                        // If there are more condition ivolving both tables,
                        // remove them from the initial join and add then to the newly created
                        Iterator<Term> it = ((InnerJoin) join).getConditions().iterator();
                        while (it.hasNext())
                        {
                            Term c = it.next();
                            if (binary_join.getInputVariables().containsAll(LogicalPlanUtil.getRelativeVariables(c.getVariablesUsed())))
                            {
                                binary_join.addCondition(c);
                                it.remove();
                            }
                        }
                        try
                        {
                            binary_join.updateOutputInfo();
                            if (join.getChildren().size() > 1)
                            {
                                join.updateOutputInfo();
                            }
                        }
                        catch (QueryCompilationException e1)
                        {
                            assert (false);
                        }
                        
                        left = binary_join;
                        break;
                    }
                    // There is no condition that contains all variables. Example R.a+S.a=T.a
                }
            }
            if (right == null || binary_join == null || join instanceof Product)
            {
                // We have product
                
                right = null;
                // pick smallest sized child other than current
                for (Operator op : children)
                {
                    if ((op.getCardinalityEstimate().equals(Size.SMALL) || op.getCardinalityEstimate().equals(Size.ONE))
                            && !op.equals(left))
                    {
                        right = op;
                        break;
                    }
                }
                if (right == null) // pick any, all are large
                {
                    // Size of children more than one
                    right = children.indexOf(left) == 0 ? children.get(1) : children.get(children.size() % children.indexOf(left));
                    
                }
                join.removeChild(left);
                join.removeChild(right);
                
                List<Operator> ops = new ArrayList<Operator>();
                ops.add(left);
                ops.add(right);
                Product product = new Product();
                for (Operator child : ops)
                    product.addChild(child);
                join.addChild(product);
                product.setCardinalityEstimate(estimateJoin(product));
                try
                {
                    product.updateOutputInfo();
                    if (join.getChildren().size() > 1)
                    {
                        join.updateOutputInfo();
                    }
                }
                catch (QueryCompilationException e)
                {
                    assert (false);
                }
                
                left = product;
            }
        }
        if (join.getChildren().size() == 1)
        {
            LogicalPlanUtil.remove(join);
        }
        
        try
        {
            // Need to update the output info of ancestors since the order of joins affects the indexes of relative variables.
            LogicalPlanUtil.updateAncestorOutputInfo(left);
        }
        catch (QueryCompilationException e)
        {
            // This should never happen
            assert (false);
        }
        
        return left;
    }
    
    /**
     * Estimates the size of the intermediate result produced by a join operator during plan execution.
     * 
     * @param operator
     *            the join operator whose intermediate result to estimate.
     * @return the size estimate.
     */
    private static Size estimateJoin(Operator operator)
    {
        if (operator instanceof Product)
        {
            List<Operator> children = operator.getChildren();
            assert (operator.getChildren().size() == 2);
            if (children.get(0).getCardinalityEstimate().equals(Size.SMALL)
                    && children.get(1).getCardinalityEstimate().equals(Size.SMALL)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.SMALL)
                    && children.get(1).getCardinalityEstimate().equals(Size.LARGE)) return Size.LARGE;
            else if (children.get(0).getCardinalityEstimate().equals(Size.LARGE)
                    && children.get(1).getCardinalityEstimate().equals(Size.SMALL)) return Size.LARGE;
            else if (children.get(0).getCardinalityEstimate().equals(Size.LARGE)
                    && children.get(1).getCardinalityEstimate().equals(Size.LARGE)) return Size.LARGE;
            
            else if (children.get(0).getCardinalityEstimate().equals(Size.ONE)
                    && children.get(1).getCardinalityEstimate().equals(Size.ONE)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.ONE)
                    && children.get(1).getCardinalityEstimate().equals(Size.SMALL)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.SMALL)
                    && children.get(1).getCardinalityEstimate().equals(Size.ONE)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.ONE)
                    && children.get(1).getCardinalityEstimate().equals(Size.LARGE)) return Size.LARGE;
            else if (children.get(0).getCardinalityEstimate().equals(Size.LARGE)
                    && children.get(1).getCardinalityEstimate().equals(Size.ONE)) return Size.LARGE;
        }
        else if (operator instanceof InnerJoin
                || ((operator instanceof OuterJoin) && ((OuterJoin) operator).getOuterJoinVariation().equals(Variation.FULL)))
        {
            List<Operator> children = operator.getChildren();
            assert (operator.getChildren().size() == 2);
            if (children.get(0).getCardinalityEstimate().equals(Size.SMALL)
                    && children.get(1).getCardinalityEstimate().equals(Size.SMALL)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.SMALL)
                    && children.get(1).getCardinalityEstimate().equals(Size.LARGE)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.LARGE)
                    && children.get(1).getCardinalityEstimate().equals(Size.SMALL)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.LARGE)
                    && children.get(1).getCardinalityEstimate().equals(Size.LARGE)) return Size.LARGE;
            
            else if (children.get(0).getCardinalityEstimate().equals(Size.ONE)
                    && children.get(1).getCardinalityEstimate().equals(Size.ONE)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.ONE)
                    && children.get(1).getCardinalityEstimate().equals(Size.SMALL)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.SMALL)
                    && children.get(1).getCardinalityEstimate().equals(Size.ONE)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.ONE)
                    && children.get(1).getCardinalityEstimate().equals(Size.LARGE)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.LARGE)
                    && children.get(1).getCardinalityEstimate().equals(Size.ONE)) return Size.SMALL;
        }
        else if ((operator instanceof OuterJoin) && ((OuterJoin) operator).getOuterJoinVariation().equals(Variation.LEFT))
        {
            assert (operator.getChildren().size() == 2);
            List<Operator> children = operator.getChildren();
            if (children.get(0).getCardinalityEstimate().equals(Size.SMALL)
                    && children.get(1).getCardinalityEstimate().equals(Size.SMALL)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.SMALL)
                    && children.get(1).getCardinalityEstimate().equals(Size.LARGE)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.LARGE)
                    && children.get(1).getCardinalityEstimate().equals(Size.SMALL)) return Size.LARGE;
            else if (children.get(0).getCardinalityEstimate().equals(Size.LARGE)
                    && children.get(1).getCardinalityEstimate().equals(Size.LARGE)) return Size.LARGE;
            
            else if (children.get(0).getCardinalityEstimate().equals(Size.ONE)
                    && children.get(1).getCardinalityEstimate().equals(Size.ONE)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.ONE)
                    && children.get(1).getCardinalityEstimate().equals(Size.SMALL)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.SMALL)
                    && children.get(1).getCardinalityEstimate().equals(Size.ONE)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.ONE)
                    && children.get(1).getCardinalityEstimate().equals(Size.LARGE)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.LARGE)
                    && children.get(1).getCardinalityEstimate().equals(Size.ONE)) return Size.LARGE;
        }
        else if ((operator instanceof OuterJoin) && ((OuterJoin) operator).getOuterJoinVariation().equals(Variation.RIGHT))
        {
            assert (operator.getChildren().size() == 2);
            List<Operator> children = operator.getChildren();
            if (children.get(0).getCardinalityEstimate().equals(Size.SMALL)
                    && children.get(1).getCardinalityEstimate().equals(Size.SMALL)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.SMALL)
                    && children.get(1).getCardinalityEstimate().equals(Size.LARGE)) return Size.LARGE;
            else if (children.get(0).getCardinalityEstimate().equals(Size.LARGE)
                    && children.get(1).getCardinalityEstimate().equals(Size.SMALL)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.LARGE)
                    && children.get(1).getCardinalityEstimate().equals(Size.LARGE)) return Size.LARGE;
            
            else if (children.get(0).getCardinalityEstimate().equals(Size.ONE)
                    && children.get(1).getCardinalityEstimate().equals(Size.ONE)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.ONE)
                    && children.get(1).getCardinalityEstimate().equals(Size.SMALL)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.SMALL)
                    && children.get(1).getCardinalityEstimate().equals(Size.ONE)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.ONE)
                    && children.get(1).getCardinalityEstimate().equals(Size.LARGE)) return Size.LARGE;
            else if (children.get(0).getCardinalityEstimate().equals(Size.LARGE)
                    && children.get(1).getCardinalityEstimate().equals(Size.ONE)) return Size.SMALL;
        }
        
        throw new AssertionError();
    }
    
    /**
     * Estimates the size of the intermediate result produced by a set operator during plan execution.
     * 
     * @param operator
     *            the set operator whose intermediate result to estimate.
     * @return the size estimate.
     */
    private static Size estimateSetOp(SetOperator operator)
    {
        if (operator.getSetOpType() == SetOpExpression.SetOpType.UNION)
        {
            List<Operator> children = operator.getChildren();
            assert (operator.getChildren().size() == 2);
            if (children.get(0).getCardinalityEstimate().equals(Size.LARGE)
                    || children.get(1).getCardinalityEstimate().equals(Size.LARGE)) return Size.LARGE;
            else return Size.SMALL;
        }
        else if (operator.getSetOpType() == SetOpExpression.SetOpType.INTERSECT)
        {
            assert (operator.getChildren().size() == 2);
            List<Operator> children = operator.getChildren();
            if (children.get(0).getCardinalityEstimate().equals(Size.SMALL)
                    || children.get(1).getCardinalityEstimate().equals(Size.SMALL)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.ONE)
                    || children.get(1).getCardinalityEstimate().equals(Size.SMALL)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.SMALL)
                    || children.get(1).getCardinalityEstimate().equals(Size.ONE)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.ONE)
                    || children.get(1).getCardinalityEstimate().equals(Size.ONE)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.LARGE)
                    && children.get(1).getCardinalityEstimate().equals(Size.LARGE)) return Size.LARGE;
        }
        else if (operator.getSetOpType() == SetOpExpression.SetOpType.EXCEPT)
        {
            assert (operator.getChildren().size() == 2);
            List<Operator> children = operator.getChildren();
            if (children.get(0).getCardinalityEstimate().equals(Size.SMALL)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.ONE)) return Size.SMALL;
            else if (children.get(0).getCardinalityEstimate().equals(Size.LARGE)
                    && children.get(1).getCardinalityEstimate().equals(Size.SMALL)) return Size.LARGE;
            else if (children.get(0).getCardinalityEstimate().equals(Size.LARGE)
                    && children.get(1).getCardinalityEstimate().equals(Size.ONE)) return Size.LARGE;
            else if (children.get(0).getCardinalityEstimate().equals(Size.LARGE)
                    && children.get(1).getCardinalityEstimate().equals(Size.LARGE)) return Size.SMALL;
        }
        
        throw new AssertionError();
    }
    
    @Override
    public Operator visitCreateSchemaObject(CreateDataObject operator)
    {
        // Order the joins of the default plans
        for (SchemaPath path : operator.getTypesToSetDefault())
        {
            LogicalPlan plan = operator.getDefaultPlan(path);
            JoinOrderRewriter.getInstance(m_uas).rewrite(plan);
        }
        return operator;
    }
    
    @Override
    public Operator visitDropSchemaObject(DropDataObject operator)
    {
        // Do nothing
        return operator;
    }
    
    @Override
    public Operator visitInsert(Insert operator)
    {
        // Order the joins of the apply plan
        JoinOrderRewriter.getInstance(m_uas).rewrite(operator.getLogicalPlansUsed().get(0));
        
        // Copy the cardinality estimate of the child operator
        operator.setCardinalityEstimate(operator.getChild().getCardinalityEstimate());
        
        return operator;
    }
    
    @Override
    public Operator visitDelete(Delete operator)
    {
        // Copy the cardinality estimate of the child operator
        operator.setCardinalityEstimate(operator.getChild().getCardinalityEstimate());
        
        return operator;
    }
    
    @Override
    public Operator visitUpdate(Update operator)
    {
        // Order the joins of the assignment plans
        for (Assignment assignment : operator.getAssignments())
        {
            JoinOrderRewriter.getInstance(m_uas).rewrite(assignment.getLogicalPlan());
        }
        
        // Copy the cardinality estimate of the child operator
        operator.setCardinalityEstimate(operator.getChild().getCardinalityEstimate());
        
        return operator;
    }
    
    @Override
    public Operator visitIndexScan(IndexScan operator)
    {
        operator.setCardinalityEstimate(Size.SMALL);
        return operator;
    }
    
    @Override
    public Operator visitAssign(Assign operator)
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Operator visitSubquery(Subquery operator)
    {
        operator.setCardinalityEstimate(operator.getChild().getCardinalityEstimate());
        
        return operator;
    }
}
