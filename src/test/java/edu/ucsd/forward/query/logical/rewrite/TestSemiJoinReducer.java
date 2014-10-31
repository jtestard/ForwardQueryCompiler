/**
 * 
 */
package edu.ucsd.forward.query.logical.rewrite;

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
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.util.NameGeneratorFactory;

/**
 * @author Yupeng Fu
 * 
 */
@Test
public class TestSemiJoinReducer extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestSemiJoinReducer.class);
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testUncorrelated() throws Exception
    {
        verify("TestSemiJoinReducer-testUncorrelated");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testCorrelated() throws Exception
    {
        verify("TestSemiJoinReducer-testCorrelated");
    }
    
    protected void verify(String relative_file_name) throws Exception
    {
        parseTestCase(this.getClass(), relative_file_name + ".xml");
        
        NameGeneratorFactory.getInstance().resetAll();
        UnifiedApplicationState uas = getUnifiedApplicationState();
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        
        String query_expr = getQueryExpression(0);
        
        List<AstTree> ast_trees = qp.parseQuery(query_expr, new LocationImpl(relative_file_name + ".xml"));
        assert (ast_trees.size() == 1);
        
        LogicalPlan actual = qp.translate(Collections.singletonList(ast_trees.get(0)), uas).get(0);
        actual = SemiJoinReducer.create().rewrite(actual);
        
        // verify rewritten plan
        LogicalPlan expected = getLogicalPlan(0);
        checkLogicalPlan(actual, expected, uas);
        
        LogicalPlan rewritten = qp.rewriteSourceAgnostic(Collections.singletonList(actual), uas).get(0);
        LogicalPlan distributed = qp.distribute(Collections.singletonList(rewritten), uas).get(0);
        PhysicalPlan physical = qp.generate(Collections.singletonList(distributed), uas).get(0);
        // Check if the physical plan is evaluated correctly
        checkPlanEvaluation(physical, uas);
    }
}
