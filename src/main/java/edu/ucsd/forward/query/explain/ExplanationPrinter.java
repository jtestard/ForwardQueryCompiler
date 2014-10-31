/**
 * 
 */
package edu.ucsd.forward.query.explain;

/**
 * Interface for generating explanations of logical and physical query plans.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface ExplanationPrinter
{
    /**
     * Converts the operator or the term to a string for debugging purposes.
     * 
     * @return the string representation of the operator or the term.
     */
    public abstract String toExplainString();
    
}
