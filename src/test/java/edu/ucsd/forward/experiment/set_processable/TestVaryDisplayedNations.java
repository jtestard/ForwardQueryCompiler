/**
 * 
 */
package edu.ucsd.forward.experiment.set_processable;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * Tests varying the number of displayed nations.
 * 
 * @author Kian Win Ong
 * 
 */
@Test(groups = edu.ucsd.forward.test.AbstractTestCase.PERFORMANCE)
public class TestVaryDisplayedNations extends AbstractSetProcessableTestCase
{
    private static final Logger       log                         = Logger.getLogger(TestVaryDisplayedNations.class);
    
//    private static final String       APPLY_PLAN_FILE_NAME        = "TestVaryDisplayedNations-ApplyPlan.xml";
//    
//    private static final String       NORMALIZED_SETS_FILE_NAME   = "TestVaryDisplayedNations-NormalizedSets.xml";
    
    private static final String       QUERY_FILE_NAME             = "TestVaryDisplayedNations-Query.xml";
    
    private static final List<String> SELECTED_NATIONS_FILE_NAMES = new ArrayList<String>();
    static
    {
        SELECTED_NATIONS_FILE_NAMES.add("selected-nations-05.xml");
        SELECTED_NATIONS_FILE_NAMES.add("selected-nations-10.xml");
        SELECTED_NATIONS_FILE_NAMES.add("selected-nations-15.xml");
        SELECTED_NATIONS_FILE_NAMES.add("selected-nations-20.xml");
        SELECTED_NATIONS_FILE_NAMES.add("selected-nations-25.xml");
    }
    
    private static final Integer      WARM_DISCARD                = 1;
    
    private static final Integer      WARM_REPEAT                 = 3;
    
    /**
     * Tests the apply-plan logical plan.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    public void testApplyPlan() throws Exception
    {
        run(QUERY_FILE_NAME, false);
    }
    
    /**
     * Tests the normalized-sets logical plan.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    public void testNormalizedSets() throws Exception
    {
        run(QUERY_FILE_NAME, true);
    }
    
    /**
     * Runs the query.
     * 
     * @param query_file_name
     *            the file name of the query.
     * @param rewrite
     *            whether to rewrite the apply-plan
     * @throws Exception
     *             if an exception occurs.
     */
    private static void run(String query_file_name, boolean rewrite) throws Exception
    {
        for (String selected_nations_file_name : SELECTED_NATIONS_FILE_NAMES)
        {
            double sum = 0.0;
            
            for (int i = 0; i < WARM_DISCARD; i++)
            {
                executeEndToEnd(query_file_name, selected_nations_file_name, null, rewrite);
            }
            
            for (int i = 0; i < WARM_REPEAT; i++)
            {
                sum += executeEndToEnd(query_file_name, selected_nations_file_name, null, rewrite);
            }
            
            double average = sum / WARM_REPEAT;
            log.info("{} {} {}", new Object[] { query_file_name, selected_nations_file_name, average });
        }
    }
    
}
