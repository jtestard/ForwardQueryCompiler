/**
 * 
 */
package edu.ucsd.forward.data.type;

import edu.ucsd.forward.data.value.StringValue;

/**
 * A string type.
 * 
 * @author Kian Win
 * @author Yupeng
 * @author Michalis Petropoulos
 */
@SuppressWarnings("serial")
public class StringType extends AbstractPrimitiveType<StringValue, String>
{
    public StringType()
    {
    }
    
    @Override
    protected String getTypeName()
    {
        return TypeEnum.STRING.getName();
    }
    
}
