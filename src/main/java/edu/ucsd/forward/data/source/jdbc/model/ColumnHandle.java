/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.model;

import edu.ucsd.app2you.util.identity.DeepEquality;
import edu.ucsd.app2you.util.identity.Immutable;
import edu.ucsd.app2you.util.logger.Logger;

/**
 * A handle to a column in a SQL table.
 * 
 * @author Kian Win
 * @author Yupeng
 */
public class ColumnHandle extends AbstractSqlHandle implements Immutable, DeepEquality
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ColumnHandle.class);
    
    private TableHandle         m_table_handle;
    
    private String              m_name;
    
    /**
     * Constructs the handle.
     * 
     * @param table
     *            - the table.
     * @param column_name
     *            - the name of the column.
     */
    public ColumnHandle(Table table, String column_name)
    {
        assert (table != null);
        assert (column_name != null);
        m_table_handle = new TableHandle(table);
        m_name = column_name;
    }
    
    /**
     * Construct the handle.
     * 
     * @param column
     *            - the column.
     */
    public ColumnHandle(Column column)
    {
        assert (column != null);
        m_table_handle = new TableHandle(column.getTable());
        m_name = column.getName();
    }
    
    @Override
    public String toSql()
    {
        return escape(m_name);
    }
    
    @Override
    public String toSql(SqlIdentifierDictionary dictionary)
    {
        return escape(truncate(m_name, dictionary));
    }
    
    @Override
    public Object[] getDeepEqualityObjects()
    {
        return new Object[] { m_table_handle, m_name };
    }
    
    /**
     * Returns the table handle.
     * 
     * @return the table handle.
     */
    public TableHandle getTableHandle()
    {
        return m_table_handle;
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
}
