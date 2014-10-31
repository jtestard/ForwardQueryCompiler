/**
 * 
 */
package edu.ucsd.forward.query.function;

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

/**
 * Tests built in functions.
 * 
 * @author Erick Zamora
 * 
 */
@Test
public class TestFunctions extends AbstractQueryTestCase
{
    private static final Logger log = Logger.getLogger(TestFunctions.class);
    
    /**
     * Tests correct output of position function.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testPositionFunction() throws Exception
    {
        verify("TestFunctions-testPositionFunctionInDb");
        verify("TestFunctions-testPositionFunctionInMemory");
    }
    
    /**
     * Tests correct output of split_part function.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testSplitPartFunction() throws Exception
    {
        verify("TestFunctions-testSplitPartFunctionInDb");
        verify("TestFunctions-testSplitPartFunctionInMemory");
    }
    
    /**
     * Tests correct output of timestamp_to_ms function.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testTimestampToMsFunction() throws Exception
    {
        verify("TestFunctions-testTimestampToMsFunctionInDb");
        verify("TestFunctions-testTimestampToMsFunctionInMemory");
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
            
            log.debug(physical_plan.getLogicalPlan().toExplainString());
            
            ConsistencyChecker.getInstance(uas).check(physical_plan.getLogicalPlan());
            DistributedNormalFormChecker.getInstance(uas).check(physical_plan.getLogicalPlan());
            
            checkPlanEvaluation(physical_plan, uas);
        }
    }
}
