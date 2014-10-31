package edu.ucsd.forward.data.source.implicit;

import edu.ucsd.forward.data.source.SchemaObject;
import edu.ucsd.forward.data.value.DataTree;

/**
 * Wrapper around a pair of schema object and data object tree.
 * 
 * @author Hongping Lim
 * 
 */
public class SessionDataObjectEntry
{
    private DataTree     m_data_object;
    
    private SchemaObject m_schema_object;
    
    /**
     * Constructor.
     * 
     * @param data_tree
     *            data tree
     * @param schema_object
     *            schema object
     */
    public SessionDataObjectEntry(DataTree data_tree, SchemaObject schema_object)
    {
        m_data_object = data_tree;
        m_schema_object = schema_object;
    }
    
    /**
     * Returns the data object.
     * 
     * @return data object
     */
    public DataTree getDataObjectTree()
    {
        return m_data_object;
    }
    
    /**
     * Returns the schema object.
     * 
     * @return schema object
     */
    public SchemaObject getSchemaObject()
    {
        return m_schema_object;
    }
}
