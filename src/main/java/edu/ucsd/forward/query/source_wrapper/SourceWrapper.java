/**
 * 
 */
package edu.ucsd.forward.query.source_wrapper;

import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceTransaction;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryResult;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * The interface for source wrappers.
 * 
 * @author Romain Vernoux
 * 
 */
public interface SourceWrapper
{
    /**
     * Translates a logical plan into a query string that can be executed in the query processor of the wrapper's data source.
     * 
     * @param plan
     *            the physical plan to translate
     * @param data_source
     *            the data source where the plan will be executed
     * @return the translated query string
     * @throws QueryExecutionException
     *             if an error occurs during the translation
     */
    public String translatePlan(PhysicalPlan plan, DataSource data_source) throws QueryExecutionException;
    
    /**
     * Executes a given query string in a given transaction and returns the corresponding result.
     * 
     * @param plan
     *            the plan corresponding to the query string to execute
     * @param query
     *            the query string to execute
     * @param transaction
     *            the transaction to execute the query into
     * @return the result of the query
     * @throws QueryExecutionException
     *             if an error occurs during execution
     */
    public QueryResult execute(PhysicalPlan plan, String query, DataSourceTransaction transaction) throws QueryExecutionException;
    
    /**
     * Takes a plan in the distributed normal form and rewrites it into the normal form required by the source before translation to
     * the native query string.
     * 
     * @param plan
     *            the plan to rewrite
     * @throws QueryCompilationException
     *             if an error occurs during the process
     */
    public void rewriteForSource(LogicalPlan plan) throws QueryCompilationException;
}
