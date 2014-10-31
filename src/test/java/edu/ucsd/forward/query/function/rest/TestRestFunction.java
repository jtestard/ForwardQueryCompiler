/**
 * 
 */
package edu.ucsd.forward.query.function.rest;

import java.util.Collections;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.value.JsonValue;
import edu.ucsd.forward.data.value.SwitchValue;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.logical.visitors.ConsistencyChecker;
import edu.ucsd.forward.query.logical.visitors.DistributedNormalFormChecker;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * Tests the REST function.
 * 
 * @author Yupeng
 * 
 */
@Test
public class TestRestFunction extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestRestFunction.class);
    
    /**
     * Tests the REST function with groupon deal service.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testGrouponDeal() throws Exception
    {
        String relative_file_name = "TestRestFunction-testGrouponDeal";
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), relative_file_name + ".xml");
        
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        String query_expr = getQueryExpression(0);
        
        AstTree ast_tree = qp.parseQuery(query_expr, new LocationImpl(relative_file_name + ".xml")).get(0);
        
        PhysicalPlan physical_plan = qp.compile(Collections.singletonList(ast_tree), uas).get(0);
        
        ConsistencyChecker.getInstance(uas).check(physical_plan.getLogicalPlan());
        DistributedNormalFormChecker.getInstance(uas).check(physical_plan.getLogicalPlan());
        
        SwitchValue result = (SwitchValue) qp.createEagerQueryResult(physical_plan, uas).getValue();
        assertEquals("success", result.getCaseName());
        assert result.getCase() instanceof JsonValue;
        assert !((JsonValue) result.getCase()).toString().isEmpty();
    }
    
}
