/**
 * 
 */
package edu.ucsd.forward.query.source_wrapper.asterix;

import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.app2you.util.logger.Logger;
import java.io.IOException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.http.impl.client.DefaultHttpClient;
import com.google.gson.Gson;

/**
 * This test case is a first attempt to communicate with a underlying AsterixDB database using AQL.
 * 
 * @author Jules Testard
 * 
 */
public class TestAsterixDBFixedAQL extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger  log   = Logger.getLogger(TestAsterixDBFixedAQL.class);
    
    /**
     * If this flag is enabled, the testcases log information useful for creating/debugging test cases.
     */
    private static final boolean DEBUG = true;
    
    /**
     * Test that we can send a Hello World style AQL query to the AsterixDB instance.
     */
    public void testHelloWorld()
    {
        
    }
    
    public String sendAsterixRequest(String url, String body)
    {
        String jsonOutput;
        try
        {
            HttpClient client = new DefaultHttpClient();
            HttpGet request = new HttpGet(url);
            request.addHeader("content-type", "application/json");
            HttpResponse result = client.execute(request);
            jsonOutput = EntityUtils.toString(result.getEntity(), "UTF-8");
        }
        catch (IOException ex)
        {
            return ex.getMessage();
        }
        return jsonOutput;
    }
    
    /**
     * Sends a JSON request to specified URL.
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
        catch (IOException ex)
        {
            return ex.getMessage();
        }
        
    }    
}
