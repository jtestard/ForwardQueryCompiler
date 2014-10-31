package edu.ucsd.forward.query.logical.visitors;

import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.query.logical.AntiSemiJoin;
import edu.ucsd.forward.query.logical.ApplyPlan;
import edu.ucsd.forward.query.logical.Assign;
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
import edu.ucsd.forward.query.logical.plan.LogicalPlan;

/**
 * The operator visitor that checks whether a logical plan is in distributed normal form. This checker makes sure that an operator
 * has a cardinality estimate and that its execution data source is set. It also checks that the logical plan starts with a send
 * plan operator.
 * 
 * Note: The purpose of this class is to assist development of rewriting rules. There is no need for this class to be used in
 * production mode.
 * 
 * @author Michalis Petropoulos
 * 
 */
public final class DistributedNormalFormChecker extends AbstractOperatorVisitor
{
    private UnifiedApplicationState m_uas;
    
    /**
     * Hidden constructor.
     * 
     * @param uas
     *            the unified application state.
     */
    private DistributedNormalFormChecker(UnifiedApplicationState uas)
    {
        assert (uas != null);
        m_uas = uas;
    }
    
    /**
     * Gets a new consistency checker instance.
     * 
     * @param uas
     *            the unified application state.
     * @return a consistency checker instance.
     */
    public static DistributedNormalFormChecker getInstance(UnifiedApplicationState uas)
    {
        return new DistributedNormalFormChecker(uas);
    }
    
    /**
     * Visits and checks the consistency of the operators in a logical plan.
     * 
     * @param plan
     *            the logical plan to check.
     */
    public void check(LogicalPlan plan)
    {
        assert (plan.getRootOperator() instanceof SendPlan);
        
        plan.getRootOperator().accept(this);
    }
    
    /**
     * Performs distributed normal form checks common to all operators in a logical plan and recursively visits the children of the
     * input operator.
     * 
     * @param operator
     *            the operator to check.
     */
    private void checkOperator(Operator operator)
    {
        // Recursively visit the children of the input operator.
        this.visitChildren(operator);
        
        // Check that the cardinality estimate is set.
        assert (operator.getCardinalityEstimate() != null);
        
        // Check that the execution data source is set and that it exists.
        assert (operator.getExecutionDataSourceName() != null);
        try
        {
            assert (m_uas.getDataSource(operator.getExecutionDataSourceName()) != null);
        }
        catch (DataSourceException e)
        {
            throw new AssertionError(e);
        }
        
        // Check that this operator is a send plan operator, if its execution data source is different that its parent's.
        Operator parent = operator.getParent();
        if (parent != null && !operator.getExecutionDataSourceName().equals(parent.getExecutionDataSourceName()))
        {
            assert (operator instanceof SendPlan || (operator instanceof Copy && ((Copy) operator).getTargetDataSourceName().equals(parent.getExecutionDataSourceName())));
        }
        
        // Check the logical plans passed to the operator as arguments
        for (LogicalPlan logical_plan : operator.getLogicalPlansUsed())
        {
            logical_plan.getRootOperator().accept(this);
            
            // Check that the root operator of the nested plan is a send plan operator.
            if (!(operator instanceof SendPlan))
            {
                assert (logical_plan.getRootOperator() instanceof SendPlan);
            }
            // If the current operator is a send plan, then the root operator of the nested plan is not a send plan operator.
            else
            {
                assert (!(logical_plan.getRootOperator() instanceof SendPlan));
            }
        }
    }
    
    @Override
    public Operator visitAntiSemiJoin(AntiSemiJoin operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitApplyPlan(ApplyPlan operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitCopy(Copy operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitEliminateDuplicates(EliminateDuplicates operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitExists(Exists operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitGround(Ground operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitGroupBy(GroupBy operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitInnerJoin(InnerJoin operator)
    {
        assert (operator.getChildren().size() == 2);
        
        // Check that variables from all child operators are used in at least one join condition.
        
        // Romain: not true anymore.
        
        // Set<Variable> condition_variables = new HashSet<Variable>();
        // boolean found = false;
        // for (Term condition : operator.getConditions())
        // {
        // found = true;
        // for (Operator child : operator.getChildren())
        // {
        // condition_variables.addAll(LogicalPlanUtil.getRelativeVariables(condition.getVariablesUsed()));
        // found = (condition_variables.retainAll(child.getOutputInfo().getVariables())) ? found && true : found && false;
        // condition_variables.clear();
        // }
        // if (found) break;
        // }
        // assert (found);
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitNavigate(Navigate operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitOffsetFetch(OffsetFetch operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitOuterJoin(OuterJoin operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitProduct(Product operator)
    {
        assert (operator.getChildren().size() == 2);
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitProject(Project operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitScan(Scan operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitSelect(Select operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitSemiJoin(SemiJoin operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitSendPlan(SendPlan operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitSetOperator(SetOperator operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitSort(Sort operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitCreateSchemaObject(CreateDataObject operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitDropSchemaObject(DropDataObject operator)
    {
        checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitInsert(Insert operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitDelete(Delete operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitUpdate(Update operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitPartitionBy(PartitionBy operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitIndexScan(IndexScan operator)
    {
        this.checkOperator(operator);
        
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
        this.checkOperator(operator);
        
        return operator;
    }
}
