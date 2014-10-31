/**
 * 
 */
package edu.ucsd.forward.query.logical;

import java.util.List;

import edu.ucsd.forward.query.logical.term.Variable;

/**
 * The interface to detect the set of variables used by an operator or a term (no duplicates).
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface VariableUsage
{
    /**
     * Returns the list of variables used by an operator or a term.
     * 
     * @return the list of variables used by an operator or a term.
     */
    public List<Variable> getVariablesUsed();
    
}
