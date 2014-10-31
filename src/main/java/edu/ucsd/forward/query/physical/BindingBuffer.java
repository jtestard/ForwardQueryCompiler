/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.Iterator;
import java.util.List;

import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Represents a buffer of bindings that is used internally by physical operators to organize intermediate results.
 * 
 * @author Michalis Petropoulos
 * @author Vicky Papavasileiou
 */
public interface BindingBuffer
{
    /**
     * Populates the buffer with bindings coming from the input operator implementation.
     * 
     * @param op_impl
     *            an operator implementation.
     * @throws QueryExecutionException
     *             if there is an error retrieving bindings.
     * @throws SuspensionException
     *             if population is suspended.
     */
    public void populate(OperatorImpl op_impl) throws QueryExecutionException, SuspensionException;
    
    /**
     * Empties the buffer and the matched bindings collection.
     */
    public void clear();
    
    /**
     * Empties the matched bindings collection.
     */
    public void clearMatchedBindings();
    
    /**
     * Returns a collection of bindings matching the input binding. The output collection consists of either left bindings or the
     * concatenation of left bindings and the input binding.
     * 
     * @param binding
     *            the binding to match.
     * @param concatenate
     *            whether to output only left bindings or the concatenation of left bindings and the right binding.
     * @return an iterator of bindings.
     * @throws QueryExecutionException
     *             if an exception is raised during term evaluation.
     */
    public Iterator<Binding> get(Binding binding, boolean concatenate) throws QueryExecutionException;
    
    /**
     * Returns the collection of bindings in the buffer.
     * 
     * @return an iterator of bindings.
     */
    public Iterator<Binding> getBindings();
    
    /**
     * Returns the collection of bindings in the buffer.
     * 
     * @return a collection of bindings.
     */
    public List<Binding> getAllBindings();    
    
    /**
     * Returns the collection of bindings in the buffer that have been matched with an input binding.
     * 
     * @return an iterator of bindings.
     */
    public Iterator<Binding> getMatchedBindings();
    
    /**
     * Returns the collection of bindings in the buffer that have been matched with an input binding.
     * 
     * @return a collection of bindings.
     */
    public List<Binding> getAllMatchedBindings();  
    
    public Iterator<Binding> getUnmatchedBindings();
    
    /**
     * Tests if the buffer is empty.
     * 
     * @return true, if the buffer is empty; false, otherwise.
     */
    public boolean isEmpty();
    
    /**
     * Adds the input binding to the buffer.
     * 
     * @param binding
     *            a binding to add.
     * @throws QueryExecutionException
     *             if an exception is raised during term evaluation.
     */
    public void add(Binding binding) throws QueryExecutionException;
    
    /**
     * Removes the bindings that match the input binding.
     * 
     * @param binding
     *            the binding to match.
     * @throws QueryExecutionException
     *             if an exception is raised during term evaluation.
     */
    public void remove(Binding binding) throws QueryExecutionException;
    
    /**
     * Returns the number of bindings in the buffer.
     * 
     * @return the number of bindings in the buffer.
     */
    public int size();
    
}
