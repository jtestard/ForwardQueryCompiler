/**
 * 
 */
package edu.ucsd.forward.data.diff;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.DataPath;
import edu.ucsd.forward.data.value.Value;

/**
 * Abstract class for implementing diffs.
 * 
 * @author Kian Win Ong
 * 
 */
public abstract class AbstractDiff implements Diff
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractDiff.class);
    
    private DataPath            m_context;
    
    private Value               m_payload;
    
    /**
     * Constructs the diff with the context and payload. The payload may be null.
     * 
     * @param context
     *            the context.
     * @param payload
     *            the payload. The payload may be null.
     */
    public AbstractDiff(DataPath context, Value payload)
    {
        assert (context != null);
        
        // FIXME: Assert that the payload is immutable
        
        m_context = context;
        m_payload = payload;
    }
    
    /**
     * 
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.diff.Diff#getContext()
     */
    @Override
    public DataPath getContext()
    {
        return m_context;
    }
    
    /**
     * 
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.diff.Diff#getPayload()
     */
    @Override
    public Value getPayload()
    {
        return m_payload;
    }
}
