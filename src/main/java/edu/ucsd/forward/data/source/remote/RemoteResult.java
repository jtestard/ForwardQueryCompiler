/**
 * 
 */
package edu.ucsd.forward.data.source.remote;

import java.util.LinkedList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.Pipeline;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.suspension.RemoteSuspensionRequest;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * A remote data source result produced by executing on a remote server. The result is under an eager-loading process that retrieves
 * the tuples from the data source in blocks. The result maintains a cursor pointing to its current tuple in the full result,
 * initially the cursor is positioned before the first tuple. A flag indicates if the data is fully loaded from the source. The
 * eager-loading strategy is currently controlled by the execution module.
 * 
 * The result is not updatable and the cursor moves forward only. Thus, it iterates through the data only once and only from the
 * first row to the last row.
 * 
 * 
 * 
 * @author Yupeng
 * 
 */
public class RemoteResult implements Pipeline
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(RemoteResult.class);
    
    private PhysicalPlan        m_physical_plan;
    
    private int                 m_position;
    
    /**
     * The bindings iterator.
     */
    private LinkedList<Binding> m_binding_list;
    
    /**
     * Indicates if the data in remote source are fully populated.
     */
    private boolean             m_populated;
    
    /**
     * Constructs the remote result.
     * 
     * @param physical_plan
     *            the physical plan to execute
     */
    public RemoteResult(PhysicalPlan physical_plan)
    {
        assert physical_plan != null;
        m_physical_plan = physical_plan;
        
        m_populated = false;
        m_position = 0;
    }
    
    /**
     * Gets the current iteration position.
     * 
     * @return the current iteration position.
     */
    public int getPosition()
    {
        return m_position;
    }
    
    /**
     * Gets the physical plan.
     * 
     * @return the physical plan.
     */
    public PhysicalPlan getPhysicalPlan()
    {
        return m_physical_plan;
    }
    
    /**
     * Adds the computed bindings to the buffer.
     * 
     * @param bindings
     *            the bindings retrieved from the remote source.
     */
    public void setBuffer(List<Binding> bindings)
    {
        assert bindings != null;
        m_binding_list.clear();
        m_binding_list.addAll(bindings);
    }
    
    /**
     * Mark the flag when the population is fully done.
     */
    public void closePopulation()
    {
        m_populated = true;
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        // Close the physical plan
        m_physical_plan.close();
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        // Get the next binding
        Binding binding = m_binding_list.poll();
        
        if (binding == null)
        {
            if (!m_populated) throw new SuspensionException(new RemoteSuspensionRequest(this));
            return null;
        }
        
        m_position++;
        
        return binding;
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        m_physical_plan.open();
        m_binding_list = new LinkedList<Binding>();
        m_populated = false;
        m_position = 0;
    }
}
