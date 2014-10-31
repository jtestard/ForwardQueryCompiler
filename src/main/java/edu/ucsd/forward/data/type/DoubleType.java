/**
 * 
 */
package edu.ucsd.forward.data.type;

import edu.ucsd.forward.data.value.DoubleValue;

/**
 * A type representing a double.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class DoubleType extends AbstractNumericType<DoubleValue, Double>
{
    public DoubleType()
    {
    }
    
    @Override
    protected String getTypeName()
    {
        return TypeEnum.DOUBLE.getName();
    }
}
