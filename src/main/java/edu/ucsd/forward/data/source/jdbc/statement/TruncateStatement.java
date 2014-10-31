/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.statement;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.jdbc.model.SqlDialect;
import edu.ucsd.forward.data.source.jdbc.model.SqlIdentifierDictionary;
import edu.ucsd.forward.data.source.jdbc.model.TableHandle;

/**
 * Represents the SQL TRUNCATE that quickly removes all data from a table.
 * 
 * @author Yupeng
 * 
 */
public class TruncateStatement extends AbstractJdbcStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TruncateStatement.class);
    
    private TableHandle         m_table_handle;
    
    /**
     * Constructs the statement.
     * 
     * @param table_handle
     *            the table to truncate
     */
    public TruncateStatement(TableHandle table_handle)
    {
        assert (table_handle != null);
        m_table_handle = table_handle;
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.source.jdbc.statement.JdbcStatement#toSql(edu.ucsd.forward.data.source.jdbc.model.SqlDialect,
     *      edu.ucsd.forward.data.source.jdbc.model.SqlIdentifierDictionary)
     */
    @Override
    public String toSql(SqlDialect sql_dialect, SqlIdentifierDictionary dictionary)
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append("TRUNCATE ");
        sb.append(m_table_handle.toSql(dictionary));
        
        return sb.toString();
    }
}
