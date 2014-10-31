/**
 * 
 */
package edu.ucsd.forward.query;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.UncheckedException;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.Pipeline;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * A lazily executed query result.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 * 
 */
public class LazyQueryResult implements QueryResult
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(LazyQueryResult.class);
    
    private Pipeline            m_pipeline;
    
    private PhysicalPlan        m_physical_plan;
    
    /**
     * Constructs a query result given a pipeline and a physical plan.
     * 
     * @param pipeline
     *            the pipeline that will lazily evaluate the query result.
     * @param physical_plan
     *            the physical plan that produced the query result.
     */
    public LazyQueryResult(Pipeline pipeline, PhysicalPlan physical_plan)
    {
        assert (pipeline != null);
        assert (physical_plan != null);
        
        m_pipeline = pipeline;
        m_physical_plan = physical_plan;
    }
    
    /**
     * Gets the physical plan under the execution.
     * 
     * @return the physical plan under the execution.
     */
    protected PhysicalPlan getPhysicalPlan()
    {
        return m_physical_plan;
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        try
        {
            m_pipeline.open();
        }
        catch (Throwable t)
        {
            // Propagate abnormal conditions immediately
            if (t instanceof java.lang.Error && !(t instanceof AssertionError)) throw (java.lang.Error) t;
            
            // Close the physical plan
            m_physical_plan.close();
            
            // Cleanup the data source accesses
            QueryProcessorFactory.getInstance().cleanup(true);
            
            // Throw the expected exception
            if (t instanceof QueryExecutionException) throw (QueryExecutionException) t;
            
            // Propagate the exception
            throw (t instanceof UncheckedException) ? (UncheckedException) t : new UncheckedException(t);
        }
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        try
        {
            return m_pipeline.next();
        }
        catch (Throwable t)
        {
            
            if (t instanceof SuspensionException) throw (SuspensionException) t;
            
            // Propagate abnormal conditions immediately
            if (t instanceof java.lang.Error && !(t instanceof AssertionError)) throw (java.lang.Error) t;
            
            // Close the physical plan
            m_physical_plan.close();
            
            // Cleanup the data source accesses
            QueryProcessorFactory.getInstance().cleanup(true);
            
            // Throw the expected exception
            if (t instanceof QueryExecutionException) throw (QueryExecutionException) t;
            
            // Propagate the exception
            throw (t instanceof UncheckedException) ? (UncheckedException) t : new UncheckedException(t);
        }
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        try
        {
            m_pipeline.close();
        }
        catch (Throwable t)
        {
            // Propagate abnormal conditions immediately
            if (t instanceof java.lang.Error && !(t instanceof AssertionError)) throw (java.lang.Error) t;
            
            // Close the physical plan
            m_physical_plan.close();
            
            // Cleanup the data source accesses
            QueryProcessorFactory.getInstance().cleanup(true);
            
            // Throw the expected exception
            if (t instanceof QueryExecutionException) throw (QueryExecutionException) t;
            
            // Propagate the exception
            throw (t instanceof UncheckedException) ? (UncheckedException) t : new UncheckedException(t);
        }
        
        // Close the physical plan
        m_physical_plan.close();
    }
    
}
