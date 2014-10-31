/**
 * 
 */
package edu.ucsd.forward.query.json;

import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.logical.visitors.ConsistencyChecker;
import edu.ucsd.forward.query.logical.visitors.DistributedNormalFormChecker;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * Tests JSON object navigation in query processing.
 * 
 * @author Yupeng
 * 
 */
@Test
public class TestJson extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestJson.class);
    
    /**
     * Tests scanning JSON value in FROM item.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testScan() throws Exception
    {
        verify("TestJson-testScan");
    }
    
    /**
     * Tests the term evaluation on JSON values.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testTerm() throws Exception
    {
        verify("TestJson-testTerm-FunctionCallMath");
        verify("TestJson-testTerm-FunctionCallComparison");
        verify("TestJson-testTerm-FunctionCallLogical");
        verify("TestJson-testTerm-FunctionCallAggregate");
    }
    
    /**
     * Tests distinct.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testDistinct() throws Exception
    {
        verify("TestJson-testDistinct");
    }
    
    /**
     * Tests group by.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testGroupBy() throws Exception
    {
        verify("TestJson-testGroupBy");
    }
    
    /**
     * Tests join.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testJoin() throws Exception
    {
        verify("TestJson-testJoin");
        verify("TestJson-testLeftOuterJoin");
    }
    
    /**
     * Tests set operation.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testSetOp() throws Exception
    {
        verify("TestJson-testSetOp");
    }
    
    /**
     * Tests offset fetch.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testOffsetFetch() throws Exception
    {
        verify("TestJson-testOffsetFetch");
    }
    
    /**
     * Tests selection.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testSelect() throws Exception
    {
        verify("TestJson-testSelect");
    }
    
    @Test(groups = AbstractTestCase.FAILURE)
    public void testJsonGetFunction() throws Exception
    {
        verify("TestJson-testJsonGetFunction");
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
        
        List<AstTree> ast_trees = qp.parseQuery(query_expr, new LocationImpl(relative_file_name + ".xml"));
        
        for (AstTree ast_tree : ast_trees)
        {
            PhysicalPlan physical_plan = qp.compile(Collections.singletonList(ast_tree), uas).get(0);
            
            ConsistencyChecker.getInstance(uas).check(physical_plan.getLogicalPlan());
            DistributedNormalFormChecker.getInstance(uas).check(physical_plan.getLogicalPlan());
            
            checkPlanEvaluation(physical_plan, uas);
        }
    }
}
