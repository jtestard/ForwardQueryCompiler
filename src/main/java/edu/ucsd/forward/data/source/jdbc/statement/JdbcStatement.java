/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.statement;

import edu.ucsd.forward.data.source.jdbc.model.SqlDialect;
import edu.ucsd.forward.data.source.jdbc.model.SqlIdentifierDictionary;

/**
 * A JDBC statement is a SQL command with named parameters. Named parameters are represented as <code>$parameter_name</code>, and
 * are bound as prepared statement parameters.
 * 
 * @author Kian Win
 * @author Yupeng
 * @author Michalis Petropoulos
 */
public interface JdbcStatement
{
    /**
     * Returns the JDBC string with named parameters. A dictionary may be needed to store the correspondence between an original
     * (pretty) SQL identifier, and a shorter version that includes a system-generated id to make the identifier unambiguous when
     * truncated.
     * 
     * @param dictionary
     *            the dictionary stores the correspondence between an original (pretty) SQL identifier, and a shorter version
     * 
     * @param sql_dialect
     *            - the SQL dialect.
     * 
     * @return the SQL string with named parameters.
     */
    public abstract String toSql(SqlDialect sql_dialect, SqlIdentifierDictionary dictionary);
}
