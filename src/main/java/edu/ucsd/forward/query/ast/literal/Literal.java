/**
 * 
 */
package edu.ucsd.forward.query.ast.literal;

import edu.ucsd.forward.query.ast.ValueExpression;

/**
 * Represents a scalar or null literal.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface Literal extends ValueExpression
{
    /**
     * Gets the literal in string format.
     * 
     * @return the literal in string format.
     */
    public String getLiteral();
    
    /**
     * Sets the literal in string format.
     * 
     * @param literal
     *            the literal in string format to set.
     */
    public void setLiteral(String literal);
    
}
