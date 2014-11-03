/**
 * 
 */
package edu.ucsd.forward.query.source_wrapper.asterix;

import org.testng.annotations.Test;

import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.app2you.util.logger.Logger;
import org.testng.annotations.Test;

/**
 * This test case is a first attempt to communicate with a underlying AsterixDB database using AQL.
 * 
 * @author Jules Testard
 * 
 */
@Test
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
    public void testHelloWorldSimple() throws Exception
    {
        //Create a AsterixDB wrapper.
        AsterixSourceWrapper wrapper = new AsterixSourceWrapper("localhost",19002);
        
        //Create a AQL request.
        String helloWorldAQLString = "let $message := 'Hello World!'\nreturn $message";
        AQLRequest request = new AQLRequest(AQLRequest.RequestType.QUERY, helloWorldAQLString);
        
        //Send the request
        String jsonOutput = wrapper.getClient().sendAQLRequest(request);
        String expectedOutput = "{\"results\":[\"\\\"Hello World!\\\"\\n\"]}";
        
        assertEquals(expectedOutput, jsonOutput);
        
        if (DEBUG) {
            log.info("Output for the request is:\n" + jsonOutput);   
        }
    }
    
    public void testHelloWorld() throws Exception
    {
        String relative_file_name = "TestAsterixDBFixedAQL-testHelloWorld";
        
        parseTestCase(this.getClass(), relative_file_name + ".xml");
    }
    
}
