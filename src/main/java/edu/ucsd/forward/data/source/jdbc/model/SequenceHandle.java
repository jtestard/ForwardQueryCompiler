/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.model;

import edu.ucsd.app2you.util.identity.DeepEquality;
import edu.ucsd.app2you.util.identity.Immutable;
import edu.ucsd.app2you.util.logger.Logger;

/**
 * A handle to the sequence of an auto-increment SQL column.
 * 
 * @author Kian Win
 * 
 */
public class SequenceHandle extends AbstractSqlHandle implements Immutable, DeepEquality
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(SequenceHandle.class);
    
    private ColumnHandle        m_column_handle;
    
    /**
     * Constructs the handle.
     * 
     * @param table
     *            - the table.
     * @param column_name
     *            - the name of the column.
     */
    public SequenceHandle(Table table, String column_name)
    {
        assert (table != null);
        assert (column_name != null);
        m_column_handle = new ColumnHandle(table, column_name);
    }
    
    /**
     * Construct the handle.
     * 
     * @param column
     *            - the column.
     */
    public SequenceHandle(Column column)
    {
        assert (column != null);
        m_column_handle = new ColumnHandle(column);
    }
    
    /**
     * Construct the handle.
     * 
     * @param column_handle
     *            - the column handle.
     */
    public SequenceHandle(ColumnHandle column_handle)
    {
        assert (column_handle != null);
        m_column_handle = column_handle;
    }
    
    /**
     * 
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.sql.SqlHandle#toSql()
     */
    @Override
    public String toSql()
    {
        // schema."tablename_colname_seq"
        TableHandle table_handle = m_column_handle.getTableHandle();
        SchemaHandle schema_handle = table_handle.getSchemaHandle();
        return schema_handle.toSql() + "." + escape(table_handle.getName() + "_" + m_column_handle.getName() + "_seq");
    }
    
    /**
     * 
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.app2you.util.identity.DeepEquality#getDeepEqualityObjects()
     */
    @Override
    public Object[] getDeepEqualityObjects()
    {
        return new Object[] { m_column_handle };
    }
    
    /**
     * Returns the column handle.
     * 
     * @return the column handle.
     */
    public ColumnHandle getColumnHandle()
    {
        return m_column_handle;
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.sql.SqlHandle#toSql(edu.ucsd.forward.sql.SqlIdentifierDictionary)
     */
    @Override
    public String toSql(SqlIdentifierDictionary dictionary)
    {
        // schema."tablename_colname_seq"
        TableHandle table_handle = m_column_handle.getTableHandle();
        SchemaHandle schema_handle = table_handle.getSchemaHandle();
        String sequence_name = table_handle.getName() + "_" + m_column_handle.getName() + "_seq";
        return schema_handle.toSql(dictionary) + "." + escape(truncate(sequence_name, dictionary));
    }
    
}
