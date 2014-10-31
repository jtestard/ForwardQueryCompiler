/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.statement;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.jdbc.model.Column;
import edu.ucsd.forward.data.source.jdbc.model.ColumnHandle;
import edu.ucsd.forward.data.source.jdbc.model.SqlDialect;
import edu.ucsd.forward.data.source.jdbc.model.SqlIdentifierDictionary;
import edu.ucsd.forward.data.source.jdbc.model.SqlPrimaryKeyConstraint;
import edu.ucsd.forward.data.source.jdbc.model.TableHandle;

/**
 * Statement for the SQL command ALTER TABLE ... SET PRIMARY KEY.
 * 
 * @author Kian Win
 * 
 */
public class CreatePrimaryKeyStatement extends AbstractJdbcStatement
{
    @SuppressWarnings("unused")
    private static final Logger     log = Logger.getLogger(CreatePrimaryKeyStatement.class);
    
    private SqlPrimaryKeyConstraint m_constraint;
    
    /**
     * Constructs the statement.
     * 
     * @param constraint
     *            - the primary key constraint.
     */
    public CreatePrimaryKeyStatement(SqlPrimaryKeyConstraint constraint)
    {
        assert (constraint != null);
        m_constraint = constraint;
    }
    
    @Override
    public String toSql(SqlDialect sql_dialect, SqlIdentifierDictionary dictionary)
    {
        StringBuilder sb = new StringBuilder();
        
        // Columns used for primary keys must be non-NULL
        for (Column column : m_constraint.getColumns())
        {
            sb.append("ALTER TABLE ");
            sb.append(new TableHandle(m_constraint.getTable()).toSql(dictionary));
            sb.append(" ALTER COLUMN ");
            sb.append(new ColumnHandle(column).toSql(dictionary));
            sb.append(" SET NOT NULL;\n");
        }
        
        sb.append("ALTER TABLE ");
        sb.append(new TableHandle(m_constraint.getTable()).toSql(dictionary));
        sb.append(" ADD PRIMARY KEY(");
        
        boolean first = true;
        for (Column column : m_constraint.getColumns())
        {
            if (first) first = false;
            else sb.append(", ");
            sb.append(new ColumnHandle(column).toSql(dictionary));
        }
        
        sb.append(");");
        
        return sb.toString();
    }
    
}
