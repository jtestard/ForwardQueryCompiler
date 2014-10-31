package edu.ucsd.forward.query.logical.rewrite;

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
import edu.ucsd.forward.query.logical.visitors.InitialNormalFormChecker;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * Test the module that pushes down the selections.
 * 
 * @author Vicky Papavasileiou
 * @author Michalis Petropoulos
 * @author Romain Vernoux
 */
@Test
public class TestPushConditionsDown extends AbstractQueryTestCase
{
    
    @SuppressWarnings("unused")
    private static final Logger  log   = Logger.getLogger(TestPushConditionsDown.class);
    
    /**
     * If this flag is enabled, the testcases log information useful for creating/debugging test cases.
     */
    private static final boolean DEBUG = false;
    
    /**
     * Tests pushing conditions under a product.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testProduct() throws Exception
    {
        verify("TestPushConditionsDown-testProduct");
    }
    
    /**
     * Tests turning a product to join.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testProductToJoinOne() throws Exception
    {
        verify("TestPushConditionsDown-testProductToJoinOne");
    }
    
    /**
     * Tests turning a product to multiple joins.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testProductToJoinMany() throws Exception
    {
        verify("TestPushConditionsDown-testProductToJoinMany");
    }
    
    /**
     * Tests pushing a condition w/o variables from top to bottom.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testConditionWithoutVariables() throws Exception
    {
        verify("TestPushConditionsDown-testConditionWithoutVariables");
        verify("TestPushConditionsDown-testConditionWithoutVariablesProduct");
        verify("TestPushConditionsDown-testConditionWithoutVariablesJoin");
    }
    
    /**
     * Tests not pushing conditions under a left outer join.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testLeftOuterJoin() throws Exception
    {
        verify("TestPushConditionsDown-testLeftOuterJoin");
    }
    
    /**
     * Tests keeping conditions in a left outer join.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testLeftOuterJoinIsNull() throws Exception
    {
        verify("TestPushConditionsDown-testLeftOuterJoinIsNull");
    }
    
    /**
     * Tests pushing conditions down through set operators.
     * 
     * @throws Exception
     *             if anything wrong happens.
     */
    public void testSetOp() throws Exception
    {
        verify("TestPushConditionDown-testUnion");
        verify("TestPushConditionDown-testSetOp");
    }
    
    /**
     * Tests pushing conditions down on a complex example.
     * 
     * @throws Exception
     *             if anything wrong happens.
     */
    public void testComplex() throws Exception
    {
        verify("TestPushConditionDown-testComplex");
    }
    
    /**
     * Verifies that the conditions are pushed down correctly.
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
        PushConditionsDownRewriter rewriter = PushConditionsDownRewriter.getInstance(uas);
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
