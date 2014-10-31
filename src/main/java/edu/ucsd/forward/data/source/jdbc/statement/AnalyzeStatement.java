/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.statement;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.jdbc.model.ColumnHandle;
import edu.ucsd.forward.data.source.jdbc.model.SqlDialect;
import edu.ucsd.forward.data.source.jdbc.model.SqlIdentifierDictionary;
import edu.ucsd.forward.data.source.jdbc.model.TableHandle;

/**
 * ANALYZE statement collects statistics about the contents of tables in the database, and stores the results in the pg_statistic
 * system catalog. Subsequently, the query planner uses these statistics to help determine the most efficient execution plans for
 * queries.
 * 
 * Note: ANALYZE statement is for PostgreSQL only. There is no ANALYZE statement in the SQL standard.
 * 
 * @author Yupeng
 * 
 */
public class AnalyzeStatement extends AbstractJdbcStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AnalyzeStatement.class);
    
    private TableHandle         m_table_handle;
    
    private List<ColumnHandle>  m_column_handles;
    
    /**
     * Constructs the statement.
     * 
     * @param table_handle
     *            the table to analyze
     **/
    public AnalyzeStatement(TableHandle table_handle)
    {
        this(table_handle, new ArrayList<ColumnHandle>());
    }
    
    /**
     * Constructs the statement.
     * 
     * @param table_handle
     *            the table to analyze
     * @param column_handles
     *            the name of a specific column to analyze. Defaults to all columns.
     */
    public AnalyzeStatement(TableHandle table_handle, List<ColumnHandle> column_handles)
    {
        assert (table_handle != null);
        assert (column_handles != null);
        
        // Check that all columns occur in the table
        for (ColumnHandle column_handle : column_handles)
        {
            assert (column_handle.getTableHandle().equals(table_handle));
        }
        
        m_table_handle = table_handle;
        m_column_handles = column_handles;
    }
    
    @Override
    public String toSql(SqlDialect sql_dialect, SqlIdentifierDictionary dictionary)
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append("ANALYZE ");
        sb.append(m_table_handle.toSql(dictionary));
        if (!m_column_handles.isEmpty())
        {
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
        }
        return sb.toString();
    }
}
