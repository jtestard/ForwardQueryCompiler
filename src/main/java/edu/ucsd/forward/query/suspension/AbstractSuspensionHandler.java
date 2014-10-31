/**
 * 
 */
package edu.ucsd.forward.query.suspension;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * Represents the abstract suspension handler. The handler handles the request in both synchronous and asynchronous way.
 * 
 * @param <R>
 *            the class of the suspension request being handled.
 * 
 * 
 * @author Yupeng
 * 
 */
public abstract class AbstractSuspensionHandler<R extends SuspensionRequest> implements SuspensionHandler
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractSuspensionHandler.class);
    
    private R                   m_suspension_request;
    
    /**
     * Constructs an abstract suspension handler for a suspension request.
     * 
     * @param request
     *            the suspension request.
     */
    protected AbstractSuspensionHandler(R request)
    {
        assert request != null;
        m_suspension_request = request;
    }
    
    /**
     * Gets the suspension request currently being handled.
     * 
     * @return the suspension request currently being handled.
     */
    public R getRequest()
    {
        return m_suspension_request;
    }
}
