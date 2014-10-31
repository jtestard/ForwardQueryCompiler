/**
 * 
 */
package edu.ucsd.forward.query.remote;

import java.util.Collections;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * Tests the plan distribution involving remote data source.
 * 
 * @author Yupeng
 * 
 */
@Test
public class TestRemotePlanDistribution extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestRemotePlanDistribution.class);
    
    @Test(groups = AbstractTestCase.FAILURE)
    public void testRemoteSourceOnly() throws Exception
    {
        verify("TestRemotePlanDistribution-testRemoteSourceOnly");
        // The sub-sendplan should be merged when their execution side are the same
        verify("TestRemotePlanDistribution-testRemoteSourceMultiple");
    }
    
    /**
     * Verifies the plan distribution output of the query processor.
     * 
     * @param relative_file_name
     *            the relative path of the XML file specifying the test case.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    protected void verify(String relative_file_name) throws Exception
    {
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), relative_file_name + ".xml");
        
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        String query_expr = getQueryExpression(0);
        AstTree ast_tree = qp.parseQuery(query_expr, new LocationImpl(relative_file_name + ".xml")).get(0);
        PhysicalPlan actual = qp.compile(Collections.singletonList(ast_tree), uas).get(0);
        
        PhysicalPlan expected = getPhysicalPlan(0);
        
        checkPhysicalPlan(actual, expected);
    }
}
