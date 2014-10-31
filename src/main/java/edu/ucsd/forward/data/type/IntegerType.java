/**
 * 
 */
package edu.ucsd.forward.data.type;

import edu.ucsd.forward.data.value.IntegerValue;

/**
 * An integer type.
 * 
 * @author Kian Win
 * @author Yupeng
 * @author Michalis Petropoulos
 */
@SuppressWarnings("serial")
public class IntegerType extends AbstractNumericType<IntegerValue, Integer>
{
    public IntegerType()
    {
    }
    
    @Override
    protected String getTypeName()
    {
        return TypeEnum.INTEGER.getName();
    }
    
}
