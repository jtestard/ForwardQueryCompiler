/**
 * 
 */
package edu.ucsd.forward.query.source_wrapper.asterix;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * The AsterixDB source wrapper.
 * 
 * @author Jules Testard
 * 
 */
public class AsterixSourceWrapper
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AsterixSourceWrapper.class);
    
    /**
     * This client is used to send requests to the AsterixDB cluster.
     */
    private AsterixDBClient     client;

    /**
     * 
     * @param host
     * @param port
     */
    public AsterixSourceWrapper(String host, int port)
    {
        this.client = new AsterixDBClient(host, port);
    }
    
    /**
     * This method will use the wrapper's client to send an AQL request.
     * 
     * @param request
     * @return
     */
    public String sendAQLRequest(AQLRequest request)
    {
        return client.sendAQLRequest(request);
    }
    
    
    /**
     * @return the client
     */
    public AsterixDBClient getClient()
    {
        return client;
    }

    /**
     * @param client the client to set
     */
    public void setClient(AsterixDBClient client)
    {
        this.client = client;
    }
}
