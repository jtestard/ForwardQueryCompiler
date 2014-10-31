/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.Iterator;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.Operator;

/**
 * Represents a nested loop operator implementation to perform an operator between two collections. The nested loop operator works
 * by fetching and buffering all bindings from the left child operator implementation (the outer loop). For each binding in the
 * right child operator implementation (inner loop), the buffer is sequentially scanned for a binding that meets the qualifier.
 * 
 * @param <O>
 *            the class of the logical operator being implemented.
 * 
 * @author Yupeng
 * 
 */
public abstract class AbstractBinaryOperatorImplNestedLoop<O extends Operator> extends AbstractBinaryOperatorImpl<O>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractBinaryOperatorImplNestedLoop.class);
    
    /**
     * Buffer to hold bindings of the left child operator implementation.
     */
    private BindingBuffer       m_left_buffer;
    
    /**
     * Iterator for the left child operator implementation buffer.
     */
    private Iterator<Binding>   m_left_iter;
    
    /**
     * Holds the current right child operator implementation binding.
     */
    private Binding             m_right_binding;
    
    /**
     * Constructs an instance of the nested loop operator implementation for a given logical operator and child operator
     * implementations.
     * 
     * @param logical
     *            a logical operator.
     * @param left_child
     *            the left child operator implementation.
     * @param right_child
     *            the right child operator implementation.
     */
    protected AbstractBinaryOperatorImplNestedLoop(O logical, OperatorImpl left_child, OperatorImpl right_child)
    {
        super(logical, left_child, right_child);
    }
    
    /**
     * Constructs an instance of the nested loop implementation for a given logical operator.
     * 
     * @param logical
     *            a logical operator.
     */
    protected AbstractBinaryOperatorImplNestedLoop(O logical)
    {
        super(logical);
    }
    
    /**
     * Gets the left buffer.
     * 
     * @return the left buffer.
     */
    protected BindingBuffer getLeftBindingBuffer()
    {
        return m_left_buffer;
    }
    
    /**
     * Sets the left buffer.
     * 
     * @param buffer
     *            the left buffer to set.
     */
    protected void setLeftBindingBuffer(BindingBuffer buffer)
    {
        m_left_buffer = buffer;
    }
    
    /**
     * Gets the left iterator.
     * 
     * @return the left iterator.
     */
    protected Iterator<Binding> getLeftIterator()
    {
        return m_left_iter;
    }
    
    /**
     * Sets the left iterator.
     * 
     * @param iterator
     *            the iterator to set.
     */
    protected void setLeftIterator(Iterator<Binding> iterator)
    {
        m_left_iter = iterator;
    }
    
    /**
     * Gets the current right binding.
     * 
     * @return the current right binding.
     */
    protected Binding getRightBinding()
    {
        return m_right_binding;
    }
    
    /**
     * Sets the current right binding.
     * 
     * @param right_binding
     *            the current right binding.
     */
    protected void setRightBinding(Binding right_binding)
    {
        m_right_binding = right_binding;
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        if (this.getState() == State.OPEN)
        {
            m_left_buffer = null;
            m_left_iter = null;
            m_right_binding = null;
        }
        
        super.close();
    }
    
}
