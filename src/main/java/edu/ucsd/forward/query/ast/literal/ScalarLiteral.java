/**
 * 
 */
package edu.ucsd.forward.query.ast.literal;

import edu.ucsd.forward.data.value.ScalarValue;

/**
 * Represents a scalar literal.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public interface ScalarLiteral extends Literal
{
    /**
     * Returns the scalar literal value.
     * 
     * @return the scalar literal value.
     */
    public ScalarValue getScalarValue();
    
    /**
     * Sets the scalar literal value.
     * 
     * @param scalar_value
     *            the scalar literal value to set.
     */
    public void setScalarValue(ScalarValue scalar_value);
    
}
