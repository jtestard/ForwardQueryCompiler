/**
 * 
 */
package edu.ucsd.forward.query.function;

import java.util.List;

import edu.ucsd.forward.query.logical.term.Term;

/**
 * Represents a function call in a query plan.
 * 
 * @param <T>
 *            the class of the function definition.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public interface FunctionCall<T extends Function> extends Term
{
    /**
     * Gets the function definition.
     * 
     * @return the function definition.
     */
    public T getFunction();
    
    /**
     * Gets the function signature.
     * 
     * @return the function signature.
     */
    public FunctionSignature getFunctionSignature();
    
    /**
     * Gets the function call arguments.
     * 
     * @return the function call arguments.
     */
    public List<Term> getArguments();
    
    /**
     * Appends an argument to the function call.
     * 
     * @param arg
     *            the function call argument to add.
     */
    public void addArgument(Term arg);
    
    /**
     * Adds an argument to the function call in the given index position.
     * 
     * @param index
     *            the index position to add the argument.
     * @param arg
     *            the function call argument to add.
     */
    public void addArgument(int index, Term arg);
    
    /**
     * Removes an argument from the function call.
     * 
     * @param arg
     *            the function call argument to remove.
     * @return the index of the removed argument.
     */
    public int removeArgument(Term arg);
    
}
