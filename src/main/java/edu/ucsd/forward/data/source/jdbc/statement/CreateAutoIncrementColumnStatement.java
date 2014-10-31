/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.statement;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.jdbc.model.ColumnHandle;
import edu.ucsd.forward.data.source.jdbc.model.SequenceHandle;
import edu.ucsd.forward.data.source.jdbc.model.SqlAutoIncrementConstraint;
import edu.ucsd.forward.data.source.jdbc.model.SqlDialect;
import edu.ucsd.forward.data.source.jdbc.model.SqlIdentifierDictionary;
import edu.ucsd.forward.data.source.jdbc.model.TableHandle;

/**
 * Statement for the SQL command ALTER TABLE ... SET AUTO_INCREMENT.
 * 
 * @author Kian Win
 * 
 */
public class CreateAutoIncrementColumnStatement extends AbstractJdbcStatement
{
    @SuppressWarnings("unused")
    private static final Logger        log = Logger.getLogger(CreateAutoIncrementColumnStatement.class);
    
    private SqlAutoIncrementConstraint m_constraint;
    
    /**
     * Constructs the statement.
     * 
     * @param constraint
     *            - the auto increment constraint.
     */
    public CreateAutoIncrementColumnStatement(SqlAutoIncrementConstraint constraint)
    {
        assert (constraint != null);
        m_constraint = constraint;
    }
    
    @Override
    public String toSql(SqlDialect sql_dialect, SqlIdentifierDictionary dictionary)
    {
        StringBuilder sb = new StringBuilder();
        
        /*
         * The following comes from Postgresql's documentation of what "CREATE TABLE tablename (colname SERIAL)" translates to.
         * http://www.postgresql.org/docs/8.4/static/datatype-numeric.html#DATATYPE-SERIAL
         */

        TableHandle table_handle = new TableHandle(m_constraint.getTable());
        ColumnHandle column_handle = new ColumnHandle(m_constraint.getColumn());
        SequenceHandle sequence_handle = new SequenceHandle(m_constraint.getColumn());
        
        // CREATE SEQUENCE tablename_colname_seq;
        sb.append("CREATE SEQUENCE ");
        sb.append(sequence_handle.toSql(dictionary));
        sb.append(";\n");
        
        // ALTER TABLE tablename ALTER COLUMN colname SET NOT NULL;
        sb.append("ALTER TABLE ");
        sb.append(table_handle.toSql(dictionary));
        sb.append(" ALTER COLUMN ");
        sb.append(column_handle.toSql(dictionary));
        sb.append(" SET NOT NULL;\n");
        
        // ALTER TABLE tablename ALTER COLUMN colname SET DEFAULT nextval('tablename_colname_seq');
        sb.append("ALTER TABLE ");
        sb.append(table_handle.toSql(dictionary));
        sb.append(" ALTER COLUMN ");
        sb.append(column_handle.toSql(dictionary));
        sb.append(" SET DEFAULT nextval('");
        sb.append(sequence_handle.toSql(dictionary));
        sb.append("');\n");
        
        // ALTER SEQUENCE tablename_colname_seq OWNED BY tablename.colname;
        sb.append("ALTER SEQUENCE ");
        sb.append(sequence_handle.toSql(dictionary));
        sb.append(" OWNED BY ");
        sb.append(table_handle.toSql(dictionary));
        sb.append(".");
        sb.append(column_handle.toSql(dictionary));
        
        return sb.toString();
    }
}
