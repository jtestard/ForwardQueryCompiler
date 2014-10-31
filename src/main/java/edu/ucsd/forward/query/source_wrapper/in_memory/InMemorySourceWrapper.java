/**
 * 
 */
package edu.ucsd.forward.query.source_wrapper.in_memory;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceTransaction;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryResult;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.source_wrapper.AbstractSourceWrapper;

/**
 * The source wrapper corresponding to the in-memory execution site.
 * 
 * @author Romain Vernoux
 * 
 */
public class InMemorySourceWrapper extends AbstractSourceWrapper
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(InMemorySourceWrapper.class);
    
    @Override
    public String translatePlan(PhysicalPlan plan, DataSource data_source) throws QueryExecutionException
    {
        // The plans executed in memory execute the physical plan directly and no module should attempt to translate them.
        throw new UnsupportedOperationException();
    }
    
    @Override
    public QueryResult execute(PhysicalPlan plan, String query, DataSourceTransaction transaction) throws QueryExecutionException
    {
        // For now, this is not a valid way to trigger the execution of a physical plan in memory.
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void rewriteForSource(LogicalPlan plan) throws QueryCompilationException
    {
        RemoveDuplicateNavigateRewriter rewriter = new RemoveDuplicateNavigateRewriter();
        rewriter.rewrite(plan);
    }
}
