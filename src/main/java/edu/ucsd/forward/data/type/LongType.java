/**
 * 
 */
package edu.ucsd.forward.data.type;

import edu.ucsd.forward.data.value.LongValue;

/**
 * A type representing a long.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 */
@SuppressWarnings("serial")
public class LongType extends AbstractNumericType<LongValue, Long>
{
    
    public LongType()
    {
    }
    
    @Override
    protected String getTypeName()
    {
        return TypeEnum.LONG.getName();
    }
}
