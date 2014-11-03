/**
 * 
 */
package edu.ucsd.forward.query.source_wrapper.asterix;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * This class describes request
 * @author Jules Testard
 *
 */
public class AQLRequest
{
    /**
     * Here are declared the types of requests that can be made
     * to a AsterixDB cluster using the AQL API.
     * 
     */
    public static enum RequestType { QUERY, UPDATE, DDL };
    
    /**
     * The host on which the AsterixDB cluster . Should be an IP address.
     */
    private String host;
    
    /**
     * Asterix Port. 
     */
    private int port;
    private String request_type;
    
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AQLRequest.class);
    
    /**
     * 
     */
    public AQLRequest()
    {
        // TODO Auto-generated constructor stub
    }
}

