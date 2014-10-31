/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc;

import java.util.Collection;
import java.util.List;

import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.SchemaObject;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.external.ExternalFunction;

/**
 * Introspect an existing database by reading the JDBC MetaData and creates a schema object for each existing SQL table.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public interface JdbcIntrospector
{
    /**
     * Introspect the existing database and returns a collection of schema objects.
     * 
     * @param exclusions
     *            what to exclude when importing an existing relational schema.
     * @return a collection of schema object, each of which represents an existing table in the database.
     * @throws DataSourceException
     *             when the encounter error during metadata reading.
     * @throws QueryExecutionException
     *             if there is an error executing a query.
     */
    public abstract Collection<SchemaObject> introspectSchemas(List<JdbcExclusion> exclusions)
            throws DataSourceException, QueryExecutionException;
    
    /**
     * Introspect the existing database and returns a collection of external functions.
     * 
     * @param exclusions
     *            what to exclude when importing an existing relational schema.
     * @return a collection of external functions defined in the database.
     * @throws DataSourceException
     *             when the encounter error during metadata reading.
     */
    public abstract Collection<ExternalFunction> introspectFunctions(List<JdbcExclusion> exclusions) throws DataSourceException;
    
}
