/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.Collections;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * Tests the physical plan builder.
 * 
 * @author Michalis Petropoulos
 * 
 */
@Test
public class TestPhysicalPlanBuilder extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestPhysicalPlanBuilder.class);
    
    /**
     * Tests the output of the physical plan builder meets the expectation.
     * 
     * @param relative_file_name
     *            the relative path of the XML file specifying the test case.
     * @throws Exception
     *             if an error occurs.
     */
    private void checkPhysicalPlan(String relative_file_name) throws Exception
    {
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), relative_file_name + ".xml");
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        
        LogicalPlan logical_plan = getLogicalPlan(0);
        // Check that the actual logical plan can be converted to physical plan
        PhysicalPlan actual = qp.generate(qp.distribute(qp.rewriteSourceAgnostic(Collections.singletonList(logical_plan), uas), uas), uas).get(0);
        
        checkPhysicalPlan(actual, getPhysicalPlan(0));
    }
    
}
