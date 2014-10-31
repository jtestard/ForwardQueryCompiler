/**
 * 
 */
package edu.ucsd.forward.data.encoding;

/**
 * @author Aditya Avinash
 * 
 */
public enum TypeCode
{
    BOOLEAN_TYPE(0X0), NUMBER_TYPE(0X1), STRING_TYPE(0X2), ENRICHED_TYPE(0X3), TUPLE_TYPE(0X4), ARRAY_TYPE(0X5), BAG_TYPE(0X6), FAR_POINTER_TYPE(
            0X7), NEAR_POINTER_TYPE(0X8), ANY_TYPE(0X9), UNION_TYPE(0X0A), INTEGER_TYPE(0X0B), LONG_TYPE(0X0C), FLOAT_TYPE(0X0D), DOUBLE_TYPE(0X0E);
    
    private final int m_value;
    
    TypeCode(int val)
    {
        m_value = val;
    }
    
    public int value()
    {
        return m_value;
    }
}
