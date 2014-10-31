/**
 * 
 */
package edu.ucsd.forward.query.suspension;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A suspension exception that contains a suspension request that requires suspension.
 * 
 * @author Yupeng
 * 
 */
public class SuspensionException extends Exception
{
    private static final long   serialVersionUID = 2L;
    
    @SuppressWarnings("unused")
    private static final Logger     log = Logger.getLogger(SuspensionException.class);
    
    private final SuspensionRequest m_request;
    
    /**
     * Constructs the suspension exception with the suspension request.
     * 
     * @param request
     *            the suspension request.
     */
    public SuspensionException(SuspensionRequest request)
    {
        super();
        assert request != null;
        m_request = request;
    }
    
    /**
     * Gets the suspension request.
     * 
     * @return the suspension request.
     */
    public SuspensionRequest getRequest()
    {
        return m_request;
    }
    
}
