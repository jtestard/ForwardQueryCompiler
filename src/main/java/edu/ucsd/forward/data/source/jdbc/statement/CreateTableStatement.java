/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.statement;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.jdbc.model.Column;
import edu.ucsd.forward.data.source.jdbc.model.ColumnHandle;
import edu.ucsd.forward.data.source.jdbc.model.SqlDialect;
import edu.ucsd.forward.data.source.jdbc.model.SqlIdentifierDictionary;
import edu.ucsd.forward.data.source.jdbc.model.Table;
import edu.ucsd.forward.data.source.jdbc.model.TableHandle;

/**
 * Statement for the SQL command CREATE TABLE.
 * 
 * @author Kian Win
 * @author Michalis Petropoulos
 * 
 */
public class CreateTableStatement extends AbstractJdbcStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(CreateTableStatement.class);
    
    private Table               m_table;
    
    /**
     * Constructs the statement.
     * 
     * @param table
     *            - the table to create.
     */
    public CreateTableStatement(Table table)
    {
        assert (table != null);
        m_table = table;
    }
    
    @Override
    public String toSql(SqlDialect sql_dialect, SqlIdentifierDictionary dictionary)
    {
        StringBuilder sb = new StringBuilder();
        
        switch (m_table.getScope())
        {
            case PERSISTENT:
                sb.append("CREATE TABLE ");
                break;
            case LOCAL_TEMPORARY:
                sb.append("CREATE LOCAL TEMPORARY TABLE ");
                break;
        }
        
        sb.append(new TableHandle(m_table).toSql(dictionary));
        sb.append(" (");
        
        boolean first = true;
        for (Column column : m_table.getColumns())
        {
            if (first) first = false;
            else sb.append(", ");
            sb.append(new ColumnHandle(column).toSql(dictionary));
            sb.append(" ");
            sb.append(column.getSqlTypeName());
            if (column.hasDefaultValue())
            {
                sb.append(" DEFAULT ");
                sb.append(column.getDefaultValue());
            }
        }
        
        sb.append(")");
        
        return sb.toString();
    }
}
