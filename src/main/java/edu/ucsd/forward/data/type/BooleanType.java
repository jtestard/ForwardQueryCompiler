/**
 * 
 */
package edu.ucsd.forward.data.type;

import edu.ucsd.forward.data.value.BooleanValue;

/**
 * A boolean type.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class BooleanType extends AbstractPrimitiveType<BooleanValue, Boolean>
{
    public BooleanType()
    {
    }
    
    @Override
    protected String getTypeName()
    {
        return TypeEnum.BOOLEAN.getName();
    }
}
