/**
 * 
 */
package edu.ucsd.forward.query.performance;

import java.util.Collections;
import java.util.List;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.util.Timer;

/**
 * The abstract class for the performance tests on query processor.
 * 
 * @author Yupeng
 * 
 */
public abstract class AbstractPerformanceTestCase extends AbstractQueryTestCase
{
    private static final Logger log     = Logger.getLogger(AbstractPerformanceTestCase.class);
    
    private static final String NEWLINE = "\n";
    
    /**
     * Run the performance test of the query specified in the XML file.
     * 
     * @param relative_file_name
     *            the relative path of the XML file specifying the test case.
     * @throws Exception
     *             if an error occurs.
     */
    protected void run(String relative_file_name) throws Exception
    {
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), relative_file_name + ".xml");
        
        // Measure the performance
        measure();
    }
    
    /**
     * Run the performance test of the query plan specified in the XML file.
     * 
     * @param relative_file_name
     *            the relative path of the XML file specifying the test case.
     * @throws Exception
     *             if an error occurs.
     */
    protected void runPhysicalPlan(String relative_file_name) throws Exception
    {
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), relative_file_name + ".xml");
        
        // Measure the performance
        measurePhysicalPlan();
    }
    
    /**
     * Measure the performance of the query specified in the XML file.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    @SuppressWarnings("unused")
    protected static void measure() throws Exception
    {
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        String query_expr = getQueryExpression(0);
        
        long time;
        Timer.reset();
        
        // Parse time
        time = System.currentTimeMillis();
        List<AstTree> ast_trees = QueryProcessorFactory.getInstance().parseQuery(query_expr, new LocationImpl(Location.UNKNOWN_PATH));
        AstTree ast_tree = ast_trees.get(0);
        Timer.inc("Parse", System.currentTimeMillis() - time);
        
        String actual_out = "";
        
        // Start compile time
        time = System.currentTimeMillis();
        
        PhysicalPlan physical_plan = QueryProcessorFactory.getInstance().compile(Collections.singletonList(ast_tree), uas).get(0);
        
        // Stop compile time
        Timer.inc("Compile", System.currentTimeMillis() - time);
        
        // Execute time
        time = System.currentTimeMillis();
        Value result = QueryProcessorFactory.getInstance().createEagerQueryResult(physical_plan, uas).getValue();
        Timer.inc("Execute", System.currentTimeMillis() - time);
        StringBuilder sb = new StringBuilder();
        
        actual_out += NEWLINE + "////////////// Query Processing //////////////" + NEWLINE;
        
        actual_out += "<---- Expression ---->\n";
        ast_tree.toQueryString(sb, 0, null);
        actual_out += sb.toString() + NEWLINE;
        
        actual_out += "<---- Physical Plan ---->\n";
        actual_out += physical_plan.toExplainString() + NEWLINE;
        
        actual_out += "Parse: " + Timer.get("Parse") + Timer.MS + NEWLINE;
        actual_out += "Compile: " + Timer.get("Compile") + Timer.MS + NEWLINE;
        actual_out += "Execute: " + Timer.get("Execute") + Timer.MS + NEWLINE + NEWLINE;
        
        actual_out += "JdbcSendPlanNext: " + Timer.get("JdbcSendPlanNext") + Timer.MS + NEWLINE + NEWLINE;
        
        actual_out += "TOTAL: " + (Timer.get("Parse") + Timer.get("Compile") + Timer.get("Execute")) + Timer.MS + NEWLINE + NEWLINE;
        
        log.info(actual_out);
    }
    
    private void measurePhysicalPlan() throws Exception
    {
        long time;
        Timer.reset();
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        PhysicalPlan physical_plan = getPhysicalPlan(0);
        
        String actual_out = null;
        
        // Execute time
        time = System.currentTimeMillis();
        QueryProcessorFactory.getInstance().createEagerQueryResult(physical_plan, uas).getValue();
        Timer.inc("Execute", System.currentTimeMillis() - time);
        
        actual_out += NEWLINE + "////////////// Query Processing //////////////" + NEWLINE;
        
        actual_out += "<---- Physical Plan ---->\n";
        actual_out += physical_plan.toExplainString() + NEWLINE;
        
        actual_out += "Parse: " + Timer.get("Parse") + Timer.MS + NEWLINE;
        actual_out += "Compile: " + Timer.get("Compile") + Timer.MS + NEWLINE;
        actual_out += "Execute: " + Timer.get("Execute") + Timer.MS + NEWLINE + NEWLINE;
        
        actual_out += "JdbcSendPlanNext: " + Timer.get("JdbcSendPlanNext") + Timer.MS + NEWLINE + NEWLINE;
        
        actual_out += "TOTAL: " + (Timer.get("Parse") + Timer.get("Compile") + Timer.get("Execute")) + Timer.MS + NEWLINE + NEWLINE;
        
        log.info(actual_out);
    }
}
