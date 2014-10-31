package edu.ucsd.forward.query.source_wrapper.in_memory;

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
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.visitors.ConsistencyChecker;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * Tests the module that removes duplicate navigate operators for the execution in memory.
 * 
 * @author Romain Vernoux
 */
@Test
public class TestRemoveDuplicateNavigateRewriter extends AbstractQueryTestCase
{
    
    @SuppressWarnings("unused")
    private static final Logger  log   = Logger.getLogger(TestRemoveDuplicateNavigateRewriter.class);
    
    /**
     * If this flag is enabled, the testcases log information useful for creating/debugging test cases.
     */
    private static final boolean DEBUG = false;
    
    /**
     * Tests remove duplicate Navigate operators in a simple query.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testSimpleQuery() throws Exception
    {
        verify("TestRemoveDuplicateNavigate-testSimpleQuery");
    }
    
    /**
     * Tests remove duplicate Navigate operators in a query with a join.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testJoin() throws Exception
    {
        verify("TestRemoveDuplicateNavigate-testJoin");
    }
    
    /**
     * Tests remove duplicate Navigate operators in a query with a Set operator.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testSetOperator() throws Exception
    {
        verify("TestRemoveDuplicateNavigate-testUnion");
    }
    
    /**
     * Verifies that the duplicate Navigate operators are removed correctly.
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
        
        if (DEBUG)
        {
            logLogicalPlanExplain(actual_input);
            logLogicalPlanXml(actual_input);
        }
        
        // Check against expected input
        LogicalPlan expected_input = getLogicalPlan(0);
        checkLogicalPlan(actual_input, expected_input, uas);
        
        // Rewrite the logical plan
        RemoveDuplicateNavigateRewriter rewriter = new RemoveDuplicateNavigateRewriter();
        rewriter.rewrite(expected_input);
        
        if (DEBUG)
        {
            logLogicalPlanExplain(expected_input);
            logLogicalPlanXml(expected_input);
        }
        
        ConsistencyChecker.getInstance(uas).check(expected_input);
        
        // Check against expected output
        LogicalPlan expected_output = getLogicalPlan(1);
        checkLogicalPlan(expected_input, expected_output, uas);
        
        // Check that the actual logical plan can be converted to physical plan
        PhysicalPlan pp = qp.generate(qp.distribute(Collections.singletonList(expected_input), uas), uas).get(0);
        
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
