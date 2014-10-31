/**
 * 
 */
package edu.ucsd.forward.query.logical.rewrite;

import java.util.Collections;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * Test the index scan rewriting.
 * 
 * @author Yupeng
 * 
 */
@Test
public class TestIndexScanRewrite extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestIndexScanRewrite.class);
    
    /**
     * Tests building index conditions under a product.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testProduct() throws Exception
    {
        verify("TestIndexScanRewrite-testProduct");
    }
    
    /**
     * Tests getting index conditions.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testConditions() throws Exception
    {
        verify("TestIndexScanRewrite-testConditions");
        verify("TestIndexScanRewrite-testConditionsFullRange");
        // The index scan operator should not be built because of the OR function in the condition.
        verify("TestIndexScanRewrite-testConditionsOr");
    }
    
    /**
     * Tests getting index conditions in EXISTS clause.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testExists() throws Exception
    {
        verify("TestIndexScanRewrite-testExists");
    }
    
    /**
     * Verifies the join ordering.
     * 
     * @param relative_file_name
     *            the relative path of the XML file specifying the test case.
     * @throws Exception
     *             if an error occurs.
     */
    protected void verify(String relative_file_name) throws Exception
    {
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), relative_file_name + ".xml");
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        
        String query_expr = getQueryExpression(0);
        
        AstTree ast_tree = qp.parseQuery(query_expr, new LocationImpl(relative_file_name + ".xml")).get(0);
        
        PhysicalPlan physical_plan = qp.compile(Collections.singletonList(ast_tree), uas).get(0);
        
        LogicalPlan expected = getLogicalPlan(0);
        LogicalPlan actual = physical_plan.getLogicalPlan();
        
        checkLogicalPlan(actual, expected, uas);
    }
}
