/**
 * 
 */
package edu.ucsd.forward.data.encoding;

/**
 * @author Aditya Avinash
 * 
 */
public enum ValueCode
{
    EMPTY_TUPLE_VALUE(0X0), DENSE_TUPLE_VALUE(0X1), SPARSE_TUPLE_VALUE(0X2), MAPPING_TUPLE_VALUE(0X3);
    
    private final int m_value;
    
    ValueCode(int val)
    {
        m_value = val;
    }
    
    public int value()
    {
        return m_value;
    }
}
