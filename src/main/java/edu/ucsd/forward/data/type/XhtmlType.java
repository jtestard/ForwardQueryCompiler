/**
 * 
 */
package edu.ucsd.forward.data.type;

import edu.ucsd.forward.data.value.XhtmlValue;

/**
 * A XHTML type.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class XhtmlType extends AbstractPrimitiveType<XhtmlValue, String>
{
    
    @Override
    protected String getTypeName()
    {
        return TypeEnum.XHTML.getName();
    }
    
    public XhtmlType()
    {
    }
}
