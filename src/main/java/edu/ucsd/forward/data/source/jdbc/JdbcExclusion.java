/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc;

import java.util.Properties;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A JDBC exclusion describing which part to exclude when importing an existing relational schema.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class JdbcExclusion
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(JdbcExclusion.class);
    
    private Type                m_type;
    
    private Properties          m_properties;
    
    /**
     * An enumeration of the exclusion types.
     */
    public enum Type
    {
        FOREIGN_KEY;
    }
    
    /**
     * Constructs a JDBC exclusion.
     * 
     * @param type
     *            the type of the exclusion.
     */
    public JdbcExclusion(Type type)
    {
        assert (type != null);
        m_type = type;
        
        m_properties = new Properties();
    }
    
    /**
     * Gets the type of the JDBC exclusion.
     * 
     * @return the type of the JDBC exclusion.
     */
    public Type getType()
    {
        return m_type;
    }
    
    /**
     * Gets the properties of the JDBC exclusion.
     * 
     * @return the properties of the JDBC exclusion.
     */
    public Properties getProperties()
    {
        return m_properties;
    }
    
}
