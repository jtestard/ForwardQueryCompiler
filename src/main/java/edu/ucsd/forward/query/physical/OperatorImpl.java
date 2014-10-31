package edu.ucsd.forward.query.physical;

import java.util.Collection;

import edu.ucsd.forward.query.explain.ExplanationPrinter;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.util.tree.TreeNode2;

/**
 * Represents an implementation of a logical operator in a physical query plan. Operator implementations implement the pipelined
 * interface:
 * <ul>
 * <li><i>open()</i> - the operator implementation can start producing values
 * <li><i>next()</i> - the operator implementation produces and returns the next value
 * <li><i>close()</i> - the operator implementation stops producing values
 * </ul>
 * 
 * New bindings are created only by certain operator implementations, such as DataObject and GroupBy, and then shared by the other
 * operator implementations.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface OperatorImpl extends TreeNode2<OperatorImpl, OperatorImpl, OperatorImpl, OperatorImpl, OperatorImpl>, Pipeline,
        PhysicalPlanUsage, ExplanationPrinter
{
    /**
     * An enumeration of operator implementation states.
     */
    public enum State
    {
        OPEN, CLOSED;
    }
    
    /**
     * Returns the logical operator counterpart of the physical operator implementation.
     * 
     * @return a logical operator.
     */
    public Operator getOperator();
    
    /**
     * Returns the name of the operator implementation.
     * 
     * @return the name of the operator implementation.
     */
    public String getName();
    
    /**
     * Returns the state of the operator implementation.
     * 
     * @return the state of the operator implementation.
     */
    public State getState();
    
    /**
     * Adds a child operator implementation.
     * 
     * @param child
     *            the child operator implementation.
     */
    public void addChild(OperatorImpl child);
    
    /**
     * Adds a child operator implementation in the given position.
     * 
     * @param index
     *            the index position.
     * @param child
     *            the child operator implementation.
     */
    public void addChild(int index, OperatorImpl child);
    
    /**
     * Removes a child operator implementation.
     * 
     * @param child
     *            the child operator implementation.
     */
    public void removeChild(OperatorImpl child);
    
    /**
     * Asking an operator implementation to accept a visitor.
     * 
     * @param visitor
     *            the visitor to accept.
     */
    public void accept(OperatorImplVisitor visitor);
    
    /**
     * Get all descendants of type SendPlanImpl.
     * 
     * @return the list with the descendants of type SendPlanImpl.
     */
    public Collection<SendPlanImpl> getSendPlanImplDescendants();
    
    /**
     * Creates a copy of the operator implementation.
     * 
     * @param context
     *            the copy context
     * @return a copied operator implementation.
     */
    public OperatorImpl copy(CopyContext context);
    
}
