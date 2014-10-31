/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * A cached result of JDBC statement execution.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public class JdbcEagerResult implements JdbcResult
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(JdbcEagerResult.class);
    
    private Iterator<Binding>   m_cursor;
    
    /**
     * Constructs a cached result using a list of bindings.
     * 
     * @param bindings
     *            the list of bindings.
     */
    public JdbcEagerResult(List<Binding> bindings)
    {
        assert (bindings != null);
        
        m_cursor = bindings.iterator();
    }
    
    /**
     * Constructs a cached result by copying tuples from a lazy result.
     * 
     * @param lazy_result
     *            - the lazy result.
     * @throws QueryExecutionException
     *             if an exception is thrown during query execution.
     */
    public JdbcEagerResult(JdbcResult lazy_result) throws QueryExecutionException
    {
        assert (lazy_result != null);
        
        List<Binding> list = new ArrayList<Binding>();
        
        Binding binding;
        try
        {
            binding = lazy_result.next();
            while (binding != null)
            {
                list.add(binding);
                binding = lazy_result.next();
            }
        }
        catch (SuspensionException e)
        {
            throw new QueryExecutionException(QueryExecution.UNSUPPORTED_SUSPENSION, this.getClass().getSimpleName());
        }
        
        m_cursor = list.iterator();
    }
    
    @Override
    public void open()
    {
        // Do nothing
    }
    
    @Override
    public Binding next()
    {
        return (m_cursor.hasNext()) ? m_cursor.next() : null;
    }
    
    @Override
    public void close()
    {
        // Do nothing
    }
    
}
