/**
 * 
 */
package edu.ucsd.forward.query.source_wrapper.asterix;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * This class describes requests that can be made to a AsterixDB cluster. This class only describes the data required to specify the
 * request but not how to execute it or on which cluster.
 * 
 * AQL request can be executed by the AsterixDB source wrapper (please refer to the AsterixSourceWrapper class for details).
 * 
 * 
 * @author Jules Testard
 * 
 */
public class AQLRequest
{
    /**
     * Here are declared the types of requests that can be made to a AsterixDB cluster using the AQL API.
     * 
     */
    public static enum RequestType
    {
        QUERY, UPDATE, DDL
    };
    
    /**
     * This string represents the string representation of the request type.
     */
    private String              requestType;
    
    /**
     * This string represent the parameter type used for the request. It depends directly on the request type.
     */
    private String              paramType;
    
    /**
     * This string represents the contents of the request. The data is expected to be in UTF-8 formatting.
     */
    private String              data;
    
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AQLRequest.class);
    
    /**
     * AQLRequest constructor.
     * 
     * @param requestType
     * @param data
     */
    public AQLRequest(RequestType requestType, String data)
    {
        switch (requestType)
        {
            case DDL:
                this.requestType = "ddl";
                this.requestType = "ddl";
                break;
            case QUERY:
                this.requestType = "query";
                this.requestType = "query";
                break;
            case UPDATE:
                this.requestType = "update";
                this.requestType = "statements";
                break;
            default:
                // Unknown requests cannot succeed.
                // Should we fail here or later?
                this.requestType = "unknown";
                this.paramType = "unknown";
                break;
        }
        this.data = data;
    }
    
    /**
     * @return the requestType
     */
    public String getRequestType()
    {
        return requestType;
    }
    
    /**
     * @return the data
     */
    public String getData()
    {
        return data;
    }
    
    /**
     * @param data
     *            the data to set
     */
    public void setData(String data)
    {
        this.data = data;
    }

    /**
     * @return the paramType
     */
    public String getParamType()
    {
        return paramType;
    }
}
