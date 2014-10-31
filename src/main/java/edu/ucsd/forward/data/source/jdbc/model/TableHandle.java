/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.model;

import edu.ucsd.app2you.util.identity.DeepEquality;
import edu.ucsd.app2you.util.identity.Immutable;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.jdbc.model.Table.TableScope;

/**
 * A handle to a SQL table.
 * 
 * @author Kian Win
 * @author Michalis Petropoulos
 * 
 */
public class TableHandle extends AbstractSqlHandle implements Immutable, DeepEquality
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TableHandle.class);
    
    /**
     * The scope of the table.
     */
    private TableScope          m_scope;
    
    private SchemaHandle        m_schema_handle;
    
    private String              m_name;
    
    /**
     * Constructs the handle.
     * 
     * @param scope
     *            - the scope of the table.
     * @param schema_name
     *            - the name of the schema.
     * @param table_name
     *            - the name of the table.
     */
    public TableHandle(TableScope scope, String schema_name, String table_name)
    {
        m_scope = scope;
        
        assert (schema_name != null);
        m_schema_handle = new SchemaHandle(schema_name);
        
        assert (table_name != null);
        m_name = table_name;
    }
    
    /**
     * Constructs the handle.
     * 
     * @param table
     *            - the table.
     */
    public TableHandle(Table table)
    {
        m_scope = table.getScope();
        m_schema_handle = new SchemaHandle(table.getSchema());
        m_name = table.getName();
    }
    
    @Override
    public String toSql()
    {
        switch (m_scope)
        {
            // In case of temporary tables, the schema handle is not used.
            case LOCAL_TEMPORARY:
                return escape(m_name);
            default:
                return m_schema_handle.toSql() + "." + escape(m_name);
        }
    }
    
    @Override
    public Object[] getDeepEqualityObjects()
    {
        return new Object[] { m_scope, m_schema_handle, m_name };
    }
    
    /**
     * Returns the schema handle.
     * 
     * @return the schema handle.
     */
    public SchemaHandle getSchemaHandle()
    {
        return m_schema_handle;
    }
    
    /**
     * Returns the name.
     * 
     * @return the name.
     */
    public String getName()
    {
        return m_name;
    }
    
    @Override
    public String toSql(SqlIdentifierDictionary dictionary)
    {
        switch (m_scope)
        {
            // In case of temporary tables, the schema handle is not used.
            case LOCAL_TEMPORARY:
                return escape(truncate(m_name, dictionary));
            default:
                return m_schema_handle.toSql(dictionary) + "." + escape(truncate(m_name, dictionary));
        }
    }
}
