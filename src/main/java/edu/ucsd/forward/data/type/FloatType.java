/**
 * 
 */
package edu.ucsd.forward.data.type;

import edu.ucsd.forward.data.value.FloatValue;

/**
 * A type representing a float.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class FloatType extends AbstractNumericType<FloatValue, Float>
{
    public FloatType()
    {
    }
    
    @Override
    protected String getTypeName()
    {
        return TypeEnum.FLOAT.getName();
    }
}
