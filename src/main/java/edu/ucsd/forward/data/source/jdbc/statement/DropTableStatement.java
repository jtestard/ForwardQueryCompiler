/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.statement;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.jdbc.model.SqlDialect;
import edu.ucsd.forward.data.source.jdbc.model.SqlIdentifierDictionary;
import edu.ucsd.forward.data.source.jdbc.model.Table;
import edu.ucsd.forward.data.source.jdbc.model.TableHandle;

/**
 * Statement for the SQL command DROP TABLE.
 * 
 * @author Kian Win
 * 
 */
public class DropTableStatement extends AbstractJdbcStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DropTableStatement.class);
    
    private TableHandle         m_table_handle;
    
    private boolean             m_strict;
    
    /**
     * Constructs the statement.
     * 
     * @param table
     *            - the table to drop.
     */
    public DropTableStatement(Table table)
    {
        assert (table != null);
        m_table_handle = new TableHandle(table);
    }
    
    /**
     * Constructs the statement.
     * 
     * @param table_handle
     *            - the table to drop.
     */
    public DropTableStatement(TableHandle table_handle)
    {
        assert (table_handle != null);
        m_table_handle = table_handle;
    }
    
    /**
     * Returns whether the statement fails when there is no existing schema to drop.
     * 
     * @return <code>true</code> if the statement fails when there is no existing schema to drop; <code>false</code> otherwise.
     */
    public boolean getStrict()
    {
        return m_strict;
    }
    
    /**
     * Sets whether the statement fails when there is no existing schema to drop.
     * 
     * @param strict
     *            - whether the statement fails when there is no existing schema to drop.
     */
    public void setStrict(boolean strict)
    {
        m_strict = strict;
    }
    
    @Override
    public String toSql(SqlDialect sql_dialect, SqlIdentifierDictionary dictionary)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP TABLE ");
        if (!m_strict) sb.append("IF EXISTS ");
        sb.append(m_table_handle.toSql(dictionary));
        sb.append(" CASCADE");
        return sb.toString();
    }
}
