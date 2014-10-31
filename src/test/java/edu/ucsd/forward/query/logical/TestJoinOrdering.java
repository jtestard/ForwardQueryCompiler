package edu.ucsd.forward.query.logical;

import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.TestEndToEnd;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.visitors.ConsistencyChecker;
import edu.ucsd.forward.query.logical.visitors.InitialNormalFormChecker;
import edu.ucsd.forward.query.logical.visitors.JoinOrderRewriter;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * Test the join ordering.
 * 
 * @author Vicky Papavasileiou
 * @author Michalis Petropoulos
 * @author Romain Vernoux
 */
@Test
public class TestJoinOrdering extends AbstractQueryTestCase
{
    /**
     * If this flag is enabled, the testcases log information useful for creating/debugging test cases.
     */
    private static final boolean DEBUG = false;
    
    @SuppressWarnings("unused")
    private static final Logger  log   = Logger.getLogger(TestEndToEnd.class);
    
    /**
     * Tests ordering of products.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testProduct() throws Exception
    {
        verify("TestJoinOrdering-testProduct");
    }
    
    /**
     * Tests ordering of small inner joins. All collections are of size small.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    @Test(groups = FAILURE)
    // Romain: in this test case, the input is something that cannot be produced by the logical plan builder. Therefore I didn't fix
    // it but left the source file around if we do a more advanced join ordering algorithm later.
    public void testJoinOrderSmall() throws Exception
    {
        verify("TestJoinOrdering-testInnerJoinSmall");
    }
    
    /**
     * Tests ordering of large inner joins. All collections are of size large.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    @Test(groups = FAILURE)
    // Romain: in this test case, the input is something that cannot be produced by the logical plan builder. Therefore I didn't fix
    // it but left the source file around if we do a more advanced join ordering algorithm later.
    public void testInnerJoinLarge() throws Exception
    {
        verify("TestJoinOrdering-testInnerJoinLarge");
    }
    
    /**
     * Tests ordering outer joins. The order of the outer join should stay unaffected. The test consists of an outer join and an
     * inner join above it.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    @Test(groups = FAILURE)
    // Romain: in this test case, the input is something that cannot be produced by the logical plan builder. Therefore I didn't fix
    // it but left the source file around if we do a more advanced join ordering algorithm later.
    public void testOuterJoin() throws Exception
    {
        verify("TestJoinOrdering-testOuterJoin");
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
        
        // Construct the logical plan
        String query_expr = getQueryExpression(0);
        List<AstTree> ast_trees = qp.parseQuery(query_expr, new LocationImpl(relative_file_name + ".xml"));
        LogicalPlan actual_input = qp.translate(ast_trees, uas).get(0);
        
        // Check against normal form
        new InitialNormalFormChecker(uas).check(actual_input);
        
        if (DEBUG)
        {
            logLogicalPlanExplain(actual_input);
            logLogicalPlanXml(actual_input);
        }
        
        // Check against expected input
        LogicalPlan expected_input = getLogicalPlan(0);
        checkLogicalPlan(actual_input, expected_input, uas);
        
        // Rewrite the logical plan
        JoinOrderRewriter rewriter = JoinOrderRewriter.getInstance(uas);
        LogicalPlan actual_output = rewriter.rewrite(expected_input);
        
        if (DEBUG)
        {
            logLogicalPlanExplain(actual_output);
            logLogicalPlanXml(actual_output);
        }
        
        ConsistencyChecker.getInstance(uas).check(actual_output);
        
        // Check against expected output
        LogicalPlan expected_output = getLogicalPlan(1);
        checkLogicalPlan(actual_output, expected_output, uas);
        
        // Check that the actual logical plan can be converted to physical plan
        PhysicalPlan pp = qp.generate(qp.distribute(Collections.singletonList(actual_output), uas), uas).get(0);
        
        Value result_value = qp.createEagerQueryResult(pp, uas).getValue();
        
        if (DEBUG)
        {
            logOutputValueExplain(result_value);
            logOutputValueXml(result_value);
            logOutputType(pp.getLogicalPlan().getOutputType());
        }
        
        checkOutputValue(result_value, uas);
    }
}
