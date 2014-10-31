/**
 * 
 */
package edu.ucsd.forward.query.suspension;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * The suspension request dispatcher is a singleton that dispatches a suspension request to the request handler.
 * 
 * @author Yupeng
 * 
 */
public final class SuspensionRequestDispatcher
{
    @SuppressWarnings("unused")
    private static final Logger                      log      = Logger.getLogger(SuspensionRequestDispatcher.class);
    
    /**
     * The singleton request dispatcher.
     */
    private static final SuspensionRequestDispatcher INSTANCE = new SuspensionRequestDispatcher();
    
    /**
     * Private constructor.
     */
    private SuspensionRequestDispatcher()
    {
        
    }
    
    /**
     * Gets the singleton instance.
     * 
     * @return the singleton instance.
     */
    public static SuspensionRequestDispatcher getInstance()
    {
        return INSTANCE;
    }
    
    /**
     * Dispatches a suspension request to the corresponding handler.
     * 
     * @param request
     *            the suspension request.
     * @return the handler that handles the request.
     */
    public SuspensionHandler dispatch(SuspensionRequest request)
    {
        if (request instanceof IndexedDbSuspensionRequest)
        {
            return new IndexedDbSuspensionHandler((IndexedDbSuspensionRequest) request);
        }
        else if(request instanceof IndexedDbSuspensionDmlRequest)
        {
            return new IndexedDbSuspensionDmlHandler((IndexedDbSuspensionDmlRequest)request);
        }
        throw new IllegalArgumentException();
    }
}
