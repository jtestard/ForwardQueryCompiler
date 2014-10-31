/**
 * 
 */
package edu.ucsd.forward.query.performance;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * The application-benchmark that tests the performance of each operator implementation.
 * 
 * @author Yupeng
 * 
 */
@Test(groups = AbstractTestCase.FAILURE)
public class TestApplication extends AbstractPerformanceTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestApplication.class);
    
    public void testGradVote() throws Exception
    {
        run("TestApplication-testGradVote");
    }
}
