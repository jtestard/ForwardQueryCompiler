package edu.ucsd.forward.query.logical.rewrite;

import java.util.Collection;
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
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.Project;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.plan.SubqueryRewriter;
import edu.ucsd.forward.query.logical.visitors.ConsistencyChecker;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.util.NameGenerator;

/**
 * Tests the module that inserts subqueries in the plan.
 * 
 * @author Romain Vernoux
 */
@Test
public class TestSubqueryRewriter extends AbstractQueryTestCase
{
    
    @SuppressWarnings("unused")
    private static final Logger  log   = Logger.getLogger(TestSubqueryRewriter.class);
    
    /**
     * If this flag is enabled, the testcases log information useful for creating/debugging test cases.
     */
    private static final boolean DEBUG = false;
    
    /**
     * Tests inserting a subquery below a Project.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testProject() throws Exception
    {
        verify("TestSubqueryRewriter-testProject", Scan.class);
        verify("TestSubqueryRewriter-testProjectParameter", Scan.class);
        verify("TestSubqueryRewriter-testProjectParameterNavigation", Scan.class);
    }
    
    /**
     * Tests inserting a subquery below a GroupBy.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testGroupBy() throws Exception
    {
        verify("TestSubqueryRewriter-testGroupBy", Scan.class);
        verify("TestSubqueryRewriter-testGroupByParameter", Scan.class);
    }
    
    /**
     * Tests inserting a subquery below a Set operator.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testSetOp() throws Exception
    {
        verify("TestSubqueryRewriter-testSetOp", Project.class);
    }
    
    /**
     * Verifies that the subquery is inserted correctly.
     * 
     * @param relative_file_name
     *            the relative path of the XML file specifying the test case.
     * @param clazz
     *            the class of the operator on top of which to insert the subquery
     * @throws Exception
     *             if an error occurs.
     */
    protected void verify(String relative_file_name, Class<? extends Operator> clazz) throws Exception
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
        Collection<? extends Operator> target_ops = expected_input.getRootOperator().getDescendantsAndSelf(clazz);
        Operator target = (Operator) target_ops.iterator().next();
        SubqueryRewriter rewriter = new SubqueryRewriter(NameGenerator.SQL_SOURCE_WRAPPER_GENERATOR);
        rewriter.insertSubquery(target);
        
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
