/**
 * 
 */
package edu.ucsd.forward.query.suspension;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.remote.RemoteResult;

/**
 * The suspension request for executing plan on remote data source.
 * 
 * @author Yupeng
 * 
 */
public class RemoteSuspensionRequest implements SuspensionRequest
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(RemoteSuspensionRequest.class);
    
    private RemoteResult        m_result;
    
    /**
     * Constructs the remote suspension request.
     * 
     * @param result
     *            the remote result that requires asynchronous execution.
     */
    public RemoteSuspensionRequest(RemoteResult result)
    {
        assert result != null;
        m_result = result;
    }
    
    /**
     * Gets the remote result that throws the request.
     * 
     * @return the remote result that throws the request.
     */
    public RemoteResult getRemoteResult()
    {
        return m_result;
    }
}
