/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.statement;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.jdbc.model.SqlDialect;
import edu.ucsd.forward.data.source.jdbc.model.SqlIdentifierDictionary;

/**
 * A generic statement is an uninterpreted string that can contain any SQL command. Named parameters are allowed in the string.
 * 
 * @author Kian Win
 * 
 */
public class GenericStatement extends AbstractJdbcStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(GenericStatement.class);
    
    private String              m_sql_string;
    
    /**
     * Constructs the statement.
     * 
     * @param sql_string
     *            - the SQL string.
     */
    public GenericStatement(String sql_string)
    {
        m_sql_string = sql_string;
    }
    
    @Override
    public String toSql(SqlDialect sql_dialect, SqlIdentifierDictionary dictionary)
    {
        return m_sql_string;
    }
}
