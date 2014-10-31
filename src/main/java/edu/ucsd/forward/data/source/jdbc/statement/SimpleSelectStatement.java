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
import edu.ucsd.forward.data.source.jdbc.model.Table;
import edu.ucsd.forward.data.source.jdbc.model.TableHandle;
import edu.ucsd.forward.query.ast.DynamicParameter;

/**
 * A simple SQL select statement that selects all the attributes from one table where the columns equal to the specified parameters.
 * 
 * @author Yupeng
 * 
 */
public class SimpleSelectStatement extends AbstractJdbcStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(SimpleSelectStatement.class);
    
    private TableHandle         m_table_handle;
    
    private List<ColumnHandle>  m_where_column_handles;
    
    /**
     * Constructs the statement that retrieves all the tuples from the table.
     * 
     * @param table
     *            - the table to delete rows into.
     */
    public SimpleSelectStatement(Table table)
    {
        this(new TableHandle(table), new ArrayList<ColumnHandle>());
    }
    
    /**
     * Constructs the statement. The WHERE clause will equate the given columns with respective parameter names that are identical
     * to the column names.
     * 
     * @param table_handle
     *            - the table to delete rows from.
     * @param where_column_handles
     *            - the columns for which values are specified.
     */
    public SimpleSelectStatement(TableHandle table_handle, List<ColumnHandle> where_column_handles)
    {
        assert (table_handle != null);
        assert where_column_handles != null;
        
        // Check that all where columns occur in the table
        for (ColumnHandle column_handle : where_column_handles)
        {
            assert (column_handle.getTableHandle().equals(table_handle));
        }
        
        m_table_handle = table_handle;
        m_where_column_handles = where_column_handles;
    }
    
    @Override
    public String toSql(SqlDialect sql_dialect, SqlIdentifierDictionary dictionary)
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append("SELECT * FROM ");
        sb.append(m_table_handle.toSql(dictionary));
        if (m_where_column_handles.size() > 0)
        {
            sb.append(" WHERE ");
            
            for (int i = 0; i < m_where_column_handles.size(); i++)
            {
                
                ColumnHandle column_handle = m_where_column_handles.get(i);
                
                if (i != 0) sb.append(" AND ");
                sb.append(column_handle.toSql(dictionary));
                sb.append(" = ");
                sb.append(DynamicParameter.SYMBOL);
            }
        }
        return sb.toString();
    }
}
