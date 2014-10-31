/**
 * 
 */
package edu.ucsd.forward.data.xml;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A class for testing the java value.
 * 
 * @author Yupeng
 * 
 */
public class TestJavaValueTuple implements Serializable
{
    @SuppressWarnings("unused")
    private static final Logger  log              = Logger.getLogger(TestJavaValueTuple.class);
    
    private static final long    serialVersionUID = 1L;
    
    private String               m_str;
    
    private Map<Integer, String> m_map;
    
    /**
     * Default constructor.
     */
    public TestJavaValueTuple()
    {
        m_map = new HashMap<Integer, String>();
    }
    
    /**
     * Sets the string.
     * 
     * @param string
     *            the string.
     */
    public void setString(String string)
    {
        m_str = string;
    }
    
    /**
     * Puts a key and value in the map.
     * 
     * @param key
     *            the key
     * @param value
     *            the value
     */
    public void put(int key, String value)
    {
        m_map.put(key, value);
    }
    
    /**
     * Gets the string attribute.
     * 
     * @return the string attribute.
     */
    public String getStringValue()
    {
        return m_str;
    }
    
    /**
     * Gets string value from the internal map.
     * 
     * @param key
     *            to key
     * @return the value that key maps to.
     */
    public String getFromMap(int key)
    {
        return m_map.get(key);
    }
}
