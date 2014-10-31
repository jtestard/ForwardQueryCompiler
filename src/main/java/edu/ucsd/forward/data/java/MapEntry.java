/**
 * 
 */
package edu.ucsd.forward.data.java;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A map entry used in mapping between Java object and SQL++.
 * 
 * @author Yupeng
 * 
 */
public class MapEntry
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(MapEntry.class);
    
    private Object              m_key;
    
    private Object              m_value;
    
    /**
     * Constructs with the key and value.
     * 
     * @param key
     *            key
     * @param value
     *            value
     */
    public MapEntry(Object key, Object value)
    {
        m_key = key;
        m_value = value;
    }
    
    /**
     * Get key.
     * 
     * @return key
     */
    public Object getKey()
    {
        return m_key;
    }
    
    /**
     * Get value.
     * 
     * @return value
     */
    public Object getValue()
    {
        return m_value;
    }
}
