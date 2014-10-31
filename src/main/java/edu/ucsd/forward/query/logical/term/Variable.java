/**
 * 
 */
package edu.ucsd.forward.query.logical.term;

/**
 * Variable interface for logical plans. Variables are introduced by the translation of an AST tree to a logical plan and are used
 * to pass types and values from one operator to another, and to enable provenance.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface Variable extends Term
{
    /**
     * Returns the name of the variable.
     * 
     * @return the name of the variable.
     */
    public String getName();
    
    /**
     * Determines whether the variable is anonymous or not, that is, whether it should be projected or not.
     * 
     * @return true, if the variable is anonymous; otherwise, false.
     */
    public boolean isAnonymous();
    
    /**
     * Gets the index of the variable in an input binding.
     * 
     * @return the index of the variable in an input binding.
     */
    public int getBindingIndex();
    
    /**
     * Sets the index of the variable in an input binding.
     * 
     * @param index
     *            the index of the variable in an input binding.
     */
    public void setBindingIndex(int index);
    
}
