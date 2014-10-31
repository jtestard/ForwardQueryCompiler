/**
 * 
 */
package edu.ucsd.forward.data.value;

import edu.ucsd.forward.data.type.IntegerType;

/**
 * An integer value.
 * 
 * @author Kian Win
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class IntegerValue extends AbstractNumericValue<IntegerType, Integer>
{
    
    private int m_integer;
    
    /**
     * 
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.value.Value#getTypeClass()
     */
    @Override
    public Class<IntegerType> getTypeClass()
    {
        return IntegerType.class;
    }
    
    /**
     * Constructs the value.
     * 
     * @param integer
     *            - the integer.
     */
    public IntegerValue(int integer)
    {
        m_integer = integer;
    }
    
    protected IntegerValue()
    {
        
    }
    
    @Override
    public Integer getObject()
    {
        return m_integer;
    }
    
    /**
     * 
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return m_integer + "";
    }
    
}
