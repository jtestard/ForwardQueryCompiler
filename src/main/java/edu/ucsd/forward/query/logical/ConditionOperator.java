package edu.ucsd.forward.query.logical;

import java.util.List;

import edu.ucsd.forward.query.logical.term.Term;

/**
 * Represents a condition operator, that is, Select and Join operators.
 * 
 * @author Michalis Petropoulos
 */
public interface ConditionOperator extends Operator
{
    /**
     * Adds a condition to the operator.
     * 
     * @param condition
     *            a condition.
     */
    public void addCondition(Term condition);
    
    /**
     * Removes a condition from the operator.
     * 
     * @param condition
     *            the condition to remove.
     */
    public void removeCondition(Term condition);
    
    /**
     * Returns the conditions of the operator.
     * 
     * @return a list of conditions.
     */
    public List<Term> getConditions();
    
    /**
     * Gets the merged condition.
     * 
     * @return the merged condition
     */
    public Term getMergedCondition();
    
}
