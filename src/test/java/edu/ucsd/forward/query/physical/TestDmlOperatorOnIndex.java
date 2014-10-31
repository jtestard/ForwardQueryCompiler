/**
 * 
 */
package edu.ucsd.forward.query.physical;

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
 * Test DML operators' effect on index.
 * 
 * @author Yupeng
 * 
 */
@Test
public class TestDmlOperatorOnIndex extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestDmlOperatorOnIndex.class);
    
    /**
     * Tests the implementation of insert.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testInsert() throws Exception
    {
        verify("TestDmlOperatorOnIndex-testInsert");
    }
    
    /**
     * Tests the implementation of delete.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testDelete() throws Exception
    {
        verify("TestDmlOperatorOnIndex-testDelete");
    }
    
    /**
     * Tests the implementation of update on the key.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testUpdate() throws Exception
    {
        verify("TestDmlOperatorOnIndex-testUpdate");
    }
    
    /**
     * Verifies the output of the query processor.
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
        
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        String query_expr = getQueryExpression(0);
        
        AstTree ast_tree = qp.parseQuery(query_expr, new LocationImpl(relative_file_name + ".xml")).get(0);
        PhysicalPlan physical_plan = qp.compile(Collections.singletonList(ast_tree), uas).get(0);
        // Perform the DML
        qp.createEagerQueryResult(physical_plan, uas).getValue();
        
        // The query on index
        query_expr = getQueryExpression(1);
        ast_tree = qp.parseQuery(query_expr, new LocationImpl(relative_file_name + ".xml")).get(0);
        physical_plan = qp.compile(Collections.singletonList(ast_tree), uas).get(0);
        
        // Check the index scan evaluation
        checkPlanEvaluation(physical_plan, uas);
        
    }
}
