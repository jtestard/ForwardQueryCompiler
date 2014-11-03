/**
 * 
 */
package edu.ucsd.forward.query.source_wrapper.asterix;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * @author Jules Testard
 * 
 */
public class AsterixDBClient
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AsterixDBClient.class);
    
    /**
     * The host on which the AsterixDB cluster runs. Should be an IP address.
     */
    private String              host;
    
    /**
     * The port on which the AsterixDB cluster expects requests. Default is 19002 on single node clusters.
     */
    private int                 port;
    
    public AsterixDBClient(String host, int port)
    {
        this.host = host;
        this.port = port;
    }
    
    /**
     * This method should return the request results in JSON format. Will use a HTTP get request under the covers. If an exception
     * occurs, the string will be the exception output.
     * 
     * @param request
     * @return
     */
    public String sendAQLRequest(AQLRequest request)
    {
        // Form request URL string from input
        String params = "";
        try
        {
            params = String.format("%s=%s", request.getParamType(), URLEncoder.encode(request.getData(), "UTF-8"));
        }
        catch (UnsupportedEncodingException e)
        {
            log.error("Error while encoding AQL request data", e);
        }
        String url = String.format("http://%s:%d/%s?%s", host, port, request.getRequestType(), params);
        return JSONGetRequest(url);
    }
    
    /**
     * Sends a JSON request to specified URL. Will use a HTTP get request under the covers. If an exception occurs, the string will
     * be the exception output.
     * 
     * @param url
     * @return
     */
    private String JSONGetRequest(String url)
    {
        try
        {
            HttpClient client = new DefaultHttpClient();
            HttpGet request = new HttpGet(url);
            request.addHeader("content-type", "application/json");
            HttpResponse result = client.execute(request);
            return EntityUtils.toString(result.getEntity(), "UTF-8");
        }
        catch (IOException e)
        {
            log.error("Error while sending JSON request", e);
            return e.getMessage();
        }
    }
}
