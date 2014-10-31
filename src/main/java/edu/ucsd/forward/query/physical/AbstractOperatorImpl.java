package edu.ucsd.forward.query.physical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.util.tree.AbstractTreeNode2;

/**
 * Represents an abstract implementation of a logical operator in a physical query plan. Operator implementations implement the
 * pipelined interface:
 * <ul>
 * <li><i>open()</i> - the operator implementation can start producing values
 * <li><i>next()</i> - the operator implementation produces and returns the next value
 * <li><i>close()</i> - the operator implementation stops producing values
 * </ul>
 * 
 * New values are created only by certain operator implementations, such as DataObject and GroupBy, and then shared by the other
 * operator implementations.
 * 
 * @param <O>
 *            the class of the logical operator being implemented.
 * 
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractOperatorImpl<O extends Operator> extends
        AbstractTreeNode2<OperatorImpl, OperatorImpl, OperatorImpl, OperatorImpl, OperatorImpl> implements OperatorImpl
{
    /**
     * The logical operator counterpart of the physical operator implementation.
     */
    private O     m_logical_operator;
    
    /**
     * The state of the operator implementation.
     */
    private State m_state;
    
    /**
     * Constructs an abstract operator implementation for a given logical operator.
     * 
     * @param logical
     *            a logical operator.
     */
    protected AbstractOperatorImpl(O logical)
    {
        this(logical, Collections.<OperatorImpl> emptyList());
    }
    
    /**
     * Constructs an abstract operator implementation for a given logical operator and child operator implementations.
     * 
     * @param logical
     *            a logical operator.
     * @param children
     *            the child operator implementations.
     */
    protected AbstractOperatorImpl(O logical, List<OperatorImpl> children)
    {
        assert (logical != null);
        
        m_logical_operator = logical;
        
        for (OperatorImpl child : children)
        {
            this.addChild(child);
        }
        
        m_state = State.CLOSED;
    }
    
    @Override
    public O getOperator()
    {
        return m_logical_operator;
    }
    
    @Override
    public String getName()
    {
        // return this.getClass().getSimpleName();
        String className = this.getClass().getName();
        return className.substring(className.lastIndexOf('.') + 1, className.length());
    }
    
    @Override
    public State getState()
    {
        return m_state;
    }
    
    @Override
    public void addChild(OperatorImpl child)
    {
        super.addChild(child);
    }
    
    @Override
    public void addChild(int index, OperatorImpl child)
    {
        super.addChild(index, child);
    }
    
    @Override
    public void removeChild(OperatorImpl child)
    {
        super.removeChild(child);
    }
    
    @Override
    public List<PhysicalPlan> getPhysicalPlansUsed()
    {
        return Collections.<PhysicalPlan> emptyList();
    }
    
    /**
     * Instantiates the bound parameters of this operator implementation.
     * 
     * @param in_binding
     *            the current input binding of this operator implementation.
     * @throws QueryExecutionException
     *             if there is an exception evaluating a parameter.
     */
    protected void instantiateBoundParameters(Binding in_binding) throws QueryExecutionException
    {
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        ParameterInstantiations param_insts = qp.getParameterInstantiations();
        for (Parameter param : this.getOperator().getBoundParametersUsed())
        {
            BindingValue value = TermEvaluator.evaluate(param.getTerm(), in_binding);
            value.resetCloned();
            param_insts.setInstantiation(param, value);
        }
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        assert (m_state == State.CLOSED);
        
        m_state = State.OPEN;
        
        for (OperatorImpl child : this.getChildren())
            child.open();
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        m_state = State.CLOSED;
        
        // Close recursively all child operators
        for (OperatorImpl child : this.getChildren())
            child.close();
        
        // Close recursively all nested plans
        for (PhysicalPlan nested_plan : this.getPhysicalPlansUsed())
            nested_plan.close();
    }
    
    @Override
    public String toExplainString()
    {
        String str = this.getName() + " - ";
        str += this.getOperator().toExplainString();
        
        return str;
    }
    
    @Override
    public String toString()
    {
        return toExplainString();
    }
    
    /**
     * Get all descendants of type SendPlanImpl.
     * 
     * @return the list with the descendants of type SendPlanImpl.
     */
    public Collection<SendPlanImpl> getSendPlanImplDescendants()
    {
        List<SendPlanImpl> list = new ArrayList<SendPlanImpl>();
        for (OperatorImpl child : getChildren())
        {
            if (child instanceof SendPlanImpl) list.add((SendPlanImpl) child);
            list.addAll(child.getSendPlanImplDescendants());
        }
        return list;
    }
    
}
