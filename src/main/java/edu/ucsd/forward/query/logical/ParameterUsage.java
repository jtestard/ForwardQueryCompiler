/**
 * 
 */
package edu.ucsd.forward.query.logical;

import java.util.List;

import edu.ucsd.forward.query.logical.term.Parameter;

/**
 * The interface to detect the list of free and bound parameters used by an operator or a term. Free parameters are not instantiated
 * by any child operator, but rather from a containing logical query plan. Bound parameters are instantiated by a child operator and
 * are found in the input info. The query path of free parameters can be either ABSOLUTE or RELATIVE, while the query path of bound
 * parameters is always RELATIVE.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface ParameterUsage
{
    /**
     * Returns the list of free parameters used by an operator or a term.
     * 
     * @return the list of free parameters used by an operator or a term.
     */
    public List<Parameter> getFreeParametersUsed();
    
    /**
     * Returns the list of bound parameters used by an operator or a term.
     * 
     * @return the list of bound parameters used by an operator or a term.
     */
    public List<Parameter> getBoundParametersUsed();
    
}
