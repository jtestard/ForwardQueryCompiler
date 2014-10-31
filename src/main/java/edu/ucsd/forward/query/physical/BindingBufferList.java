/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.term.Term;

/**
 * The buffer is implemented as a simple List.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class BindingBufferList extends AbstractBindingBuffer
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(BindingBufferList.class);
    
    /**
     * Buffer to hold bindings of an operator implementation.
     */
    private List<Binding>       m_buffer;
    
    /**
     * Constructor.
     * 
     * @param conditions
     *            the conditions used to match bindings.
     * @param left_info
     *            the output info of the bindings being indexed.
     * @param right_info
     *            the output info of the bindings probing the index.
     * @param track_matched_bindings
     *            determines whether to keep track of matched bindings.
     */
    protected BindingBufferList(Collection<Term> conditions, OutputInfo left_info, OutputInfo right_info,
            boolean track_matched_bindings)
    {
        super(conditions, left_info, right_info, track_matched_bindings);
        
        m_buffer = new ArrayList<Binding>();
    }
    
    @Override
    public void clear()
    {
        super.clear();
        
        m_buffer.clear();
    }
    
    @Override
    public Iterator<Binding> get(Binding binding, boolean concatenate) throws QueryExecutionException
    {
        // Apply the conditions
        return this.applyConditions(m_buffer, binding, concatenate).iterator();
    }
    
    @Override
    public Iterator<Binding> getBindings()
    {
        return m_buffer.iterator();
    }
    
    @Override
    public boolean isEmpty()
    {
        return m_buffer.isEmpty();
    }
    
    @Override
    public void add(Binding binding)
    {
        m_buffer.add(binding);
    }
    
    @Override
    public void remove(Binding binding)
    {
        // Use iterator instead of bucket.remove(binding) in order to force object equality (==).
        Iterator<Binding> iter = m_buffer.iterator();
        boolean found = false;
        while (iter.hasNext())
        {
            if (iter.next() == binding)
            {
                iter.remove();
                found = true;
                break;
            }
        }
        assert (found);
    }
    
    @Override
    public int size()
    {
        return m_buffer.size();
    }

    /**
     * <b>Inherited JavaDoc:</b><br>
     * {@inheritDoc}
     * <br><br>
     * <b>See original method below.</b>
     * <br>
     * @see edu.ucsd.forward.query.physical.BindingBuffer#getAllBindings()
     */
    @Override
    public List<Binding> getAllBindings()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * <b>Inherited JavaDoc:</b><br>
     * {@inheritDoc}
     * <br><br>
     * <b>See original method below.</b>
     * <br>
     * @see edu.ucsd.forward.query.physical.BindingBuffer#getUnmatchedBindings()
     */
    @Override
    public Iterator<Binding> getUnmatchedBindings()
    {
        // TODO Auto-generated method stub
        return null;
    }
    
}
