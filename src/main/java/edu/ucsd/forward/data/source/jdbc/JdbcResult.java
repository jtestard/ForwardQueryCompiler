/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc;

import edu.ucsd.forward.query.physical.Pipeline;

/**
 * A data source result produced by executing a JDBC statement.
 * 
 * The underlying implementation may produce the result either eagerly or lazily.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public interface JdbcResult extends Pipeline
{
}
