/**
 * 
 */
package edu.ucsd.forward.query.source_wrapper.sql;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceMetaData.StorageSystem;
import edu.ucsd.forward.data.source.DataSourceTransaction;
import edu.ucsd.forward.data.source.jdbc.JdbcExecution;
import edu.ucsd.forward.data.source.jdbc.JdbcResult;
import edu.ucsd.forward.data.source.jdbc.JdbcTransaction;
import edu.ucsd.forward.data.source.jdbc.statement.GenericStatement;
import edu.ucsd.forward.query.LazyQueryResult;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryResult;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.source_wrapper.AbstractSourceWrapper;

/**
 * The PostgreSQL source wrapper.
 * 
 * @author Romain Vernoux
 * 
 */
public class SqlSourceWrapper extends AbstractSourceWrapper
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(SqlSourceWrapper.class);
    
    @Override
    public String translatePlan(PhysicalPlan plan, DataSource data_source) throws QueryExecutionException
    {
        assert (plan != null);
        assert (data_source != null);
        assert (data_source.getMetaData().getStorageSystem().equals(StorageSystem.JDBC));
        
        PlanToAstTranslator translator = new PlanToAstTranslator();
        StringBuilder sb = new StringBuilder();
        translator.translate(plan.getLogicalPlan(), data_source).toQueryString(sb, 0, data_source);
        return sb.toString();
    }
    
    @Override
    public QueryResult execute(PhysicalPlan plan, String query, DataSourceTransaction transaction) throws QueryExecutionException
    {
        assert (query != null);
        assert (transaction != null && transaction instanceof JdbcTransaction);
        
        // Build the query statement
        GenericStatement statement = new GenericStatement(query);
        
        // Create the SQL execution
        JdbcExecution execution = new JdbcExecution((JdbcTransaction) transaction, statement);
        
        // / Run the statement
        JdbcResult result = execution.run();
        
        // FIXME Find a spot to end the SQL execution that will close the statement.
        // execution.end();
        
        return new LazyQueryResult(result, plan);
    }
    
    @Override
    public void rewriteForSource(LogicalPlan plan) throws QueryCompilationException
    {
        SqlNormalFormRewriter rewriter = new SqlNormalFormRewriter();
        rewriter.rewrite(plan);
    }
}
