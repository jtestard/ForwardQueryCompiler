/**
 * 
 */
package edu.ucsd.forward.query;

import edu.ucsd.forward.query.physical.Pipeline;

/**
 * A query result produced by executing a statement. The underlying implementation may produce the result either eagerly or lazily.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 */
public interface QueryResult extends Pipeline
{
        
}
