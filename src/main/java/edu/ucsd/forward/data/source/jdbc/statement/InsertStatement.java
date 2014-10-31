/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.statement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.jdbc.model.Column;
import edu.ucsd.forward.data.source.jdbc.model.ColumnHandle;
import edu.ucsd.forward.data.source.jdbc.model.SqlDialect;
import edu.ucsd.forward.data.source.jdbc.model.SqlIdentifierDictionary;
import edu.ucsd.forward.data.source.jdbc.model.Table;
import edu.ucsd.forward.data.source.jdbc.model.TableHandle;
import edu.ucsd.forward.query.ast.DynamicParameter;

/**
 * Statement for the SQL command INSERT.
 * 
 * @author Kian Win
 * @author Yupeng
 */
public class InsertStatement extends AbstractJdbcStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(InsertStatement.class);
    
    private TableHandle         m_table_handle;
    
    private List<ColumnHandle>  m_column_handles;
    
    /**
     * Constructs the statement. All columns of the table will receive values from the inserted rows, and inserted rows will have
     * identical column names.
     * 
     * @param table
     *            - the table to insert rows into.
     */
    public InsertStatement(Table table)
    {
        this(new TableHandle(table), getColumnHandles(table));
    }
    
    /**
     * Constructs the statement. Only the given columns will receive values from the inserted rows; other columns will receive
     * default values. The inserted rows will have identical column names.
     * 
     * @param table_handle
     *            - the table to insert rows into.
     * @param column_handles
     *            - the columns for which values are specified.
     */
    public InsertStatement(TableHandle table_handle, List<ColumnHandle> column_handles)
    {
        assert (table_handle != null);
        assert (column_handles != null);
        assert (column_handles.size() > 0);
        
        // Check that all columns occur in the table
        for (ColumnHandle column_handle : column_handles)
        {
            assert (column_handle.getTableHandle().equals(table_handle));
        }
        
        m_table_handle = table_handle;
        m_column_handles = column_handles;
    }
    
    /**
     * Returns column handles for respective columns in a table.
     * 
     * @param table
     *            - the table.
     * @return the column handles.
     */
    private static List<ColumnHandle> getColumnHandles(Table table)
    {
        List<ColumnHandle> list = new ArrayList<ColumnHandle>();
        for (Column column : table.getColumns())
        {
            list.add(new ColumnHandle(column));
        }
        return list;
    }
    
    /**
     * Returns the column handles.
     * 
     * @return the column handles.
     */
    public List<ColumnHandle> getColumnHandles()
    {
        return Collections.unmodifiableList(m_column_handles);
    }
    
    @Override
    public String toSql(SqlDialect sql_dialect, SqlIdentifierDictionary dictionary)
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append("INSERT INTO ");
        sb.append(m_table_handle.toSql(dictionary));
        
        sb.append("(");
        {
            boolean first = true;
            for (ColumnHandle column : m_column_handles)
            {
                if (!first) sb.append(", ");
                if (first) first = false;
                sb.append(column.toSql(dictionary));
            }
        }
        sb.append(") ");
        
        sb.append("VALUES (");
        {
            boolean first = true;
            for (int i = 0; i < m_column_handles.size(); i++)
            {
                if (!first) sb.append(", ");
                if (first) first = false;
                sb.append(DynamicParameter.SYMBOL);
            }
        }
        sb.append(")");
        
        return sb.toString();
    }
    
}
