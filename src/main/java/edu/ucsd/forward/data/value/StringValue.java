/**
 * 
 */
package edu.ucsd.forward.data.value;

import edu.ucsd.forward.data.type.StringType;

/**
 * A string value.
 * 
 * @author Kian Win
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class StringValue extends AbstractPrimitiveValue<StringType, String>
{
    
    private String m_string;
    
    @Override
    public Class<StringType> getTypeClass()
    {
        return StringType.class;
    }
    
    /**
     * Constructs the value.
     * 
     * @param string
     *            - the string.
     */
    public StringValue(String string)
    {
        assert (string != null);
        m_string = string;
    }
    
    protected StringValue()
    {
        
    }
    
    @Override
    public String getObject()
    {
        return m_string;
    }
    
    @Override
    public String toString()
    {
        return m_string;
    }
    
}
