/**
 * 
 */
package edu.ucsd.forward.data.source;

import java.io.Serializable;

import edu.ucsd.app2you.util.identity.DeepEquality;
import edu.ucsd.app2you.util.identity.Immutable;
import edu.ucsd.app2you.util.logger.Logger;

/**
 * A handle to a schema object.
 * 
 * @author Kian Win
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class SchemaObjectHandle implements Immutable, DeepEquality, Serializable
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(SchemaObjectHandle.class);
    
    /**
     * The hosting data source.
     */
    private String              m_data_source_name;
    
    private String              m_schema_object_name;
    
    /**
     * Constructs the handle.
     * 
     * @param data_source_name
     *            - the name of the data source.
     * @param schema_object_name
     *            - the name of the schema object.
     */
    public SchemaObjectHandle(String data_source_name, String schema_object_name)
    {
        assert (data_source_name != null);
        m_data_source_name = data_source_name;
        
        assert (schema_object_name != null);
        m_schema_object_name = schema_object_name;
    }
    
    /**
     * Default constructor.
     */
    public SchemaObjectHandle()
    {
        
    }
    
    /**
     * Returns the data source name.
     * 
     * @return the data source name.
     */
    public String getDataSourceName()
    {
        return m_data_source_name;
    }
    
    /**
     * Returns the schema object name.
     * 
     * @return the schema object name.
     */
    public String getSchemaObjectName()
    {
        return m_schema_object_name;
    }
    
    @Override
    public Object[] getDeepEqualityObjects()
    {
        return new Object[] { m_data_source_name, m_schema_object_name };
    }
    
}
