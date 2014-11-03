/**
 * 
 */
package edu.ucsd.forward.query.source_wrapper.asterix;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.logical.SendPlan;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.source_wrapper.sql.PlanToAstTranslator;

/**
 * @author Jules Testard
 *
 */
@Test
public class TestPlanToAstTranslator extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger  log   = Logger.getLogger(TestPlanToAstTranslator.class);
    
    /**
     * If this flag is enabled, the testcases log information useful for creating/debugging test cases.
     */
    private static final boolean DEBUG = true;
    
//    /**
//     * Tests the translation of terms.
//     * 
//     * @throws Exception
//     *             when encounters error
//     */
//    public void testTerm() throws Exception
//    {
//        checkTranslation("TestPlanToAstTranslator-testTerm");
//    }
//   

    /**
     * Tests the translation of the project operator.
     * 
     * @throws Exception
     *            when encounters error
     */
    public void testProject() throws Exception
    {
        checkTranslation("TestPlanToAstTranslator-testProject");
    }
    
    /**
     * Tests the translation of the physical plan meets the expectation.
     * 
     * @param relative_file_name
     *            the relative path of the XML file specifying the test case.
     * @throws Exception
     *             if an error occurs.
     */
    private void checkTranslation(String relative_file_name) throws Exception
    {
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), relative_file_name + ".xml");
        
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        UnifiedApplicationState uas = getUnifiedApplicationState();
        DataSource source = uas.getDataSource("src_1");
        
        // Parse the query
        String query_expr = getQueryExpression(0);
        List<AstTree> ast_trees = qp.parseQuery(query_expr, new LocationImpl(relative_file_name + ".xml"));
        assert (ast_trees.size() == 1);
        
        // Translate the AST
        LogicalPlan actual_input = qp.translate(Collections.singletonList(ast_trees.get(0)), uas).get(0);
        actual_input = qp.distribute(Collections.singletonList(actual_input), uas).get(0);
        
        if (DEBUG)
        {
            log.info(logStringSectionWrap("ACTUAL INPUT EXPLAIN STRING (LOGICAL PLAN) : "));
            logLogicalPlanExplain(actual_input);
            log.info(logStringSectionWrap("ACTUAL INPUT XML STRING (LOGICAL PLAN) : "));
            logLogicalPlanXml(actual_input);
        }
        
        LogicalPlan expected_input = getLogicalPlan(0);
        checkLogicalPlan(actual_input, expected_input, uas);
        
        // Translate the inner plan of the root send plan operator
        assert (actual_input.getRootOperator() instanceof SendPlan);
        LogicalPlan nested_plan = ((SendPlan) expected_input.getRootOperator()).getLogicalPlansUsed().get(0);
        StringBuilder sb = new StringBuilder();
        PlanToAstTranslator p = new PlanToAstTranslator();
        p.translate(nested_plan, source).toQueryString(sb, 0, source);
        String actual_query = sb.toString().trim();
        
        String expected_query = getQueryExpression(1).trim();
        assertEquals(expected_query, actual_query);
        
        // Checks that Postgres can run the generated plan
        PhysicalPlan pp = qp.generate(Arrays.asList(expected_input), uas).get(0);
        Value output_value = qp.createEagerQueryResult(pp, uas).getValue();
        if (DEBUG)
        {
            log.info(logStringSectionWrap("OUTPUT VALUE EXPLAIN STRING: "));
            logOutputValueExplain(output_value);
            log.info(logStringSectionWrap("OUTPUT VALUE XML STRING: "));
            logOutputValueXml(output_value);
            log.info(logStringSectionWrap("OUTPUT TYPE STRING: "));
            logOutputType(pp.getLogicalPlan().getOutputType());
        }
        checkOutputValue(output_value, uas);
    }
    
    private String logStringSectionWrap(String s) {
        return "\n========\n" + s + "\n========";
    }

}
