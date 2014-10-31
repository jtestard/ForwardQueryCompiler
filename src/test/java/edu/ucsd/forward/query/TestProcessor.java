/**
 * 
 */
package edu.ucsd.forward.query;

import static edu.ucsd.forward.xml.XmlUtil.createDocument;

import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;

import com.google.gwt.xml.client.Document;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.data.xml.ValueXmlSerializer;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.logical.visitors.ConsistencyChecker;
import edu.ucsd.forward.query.logical.visitors.DistributedNormalFormChecker;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * End-to-end test cases of the query processor.
 * 
 * @author Yupeng
 * 
 * 
 */
@Test
public class TestProcessor extends AbstractQueryTestCase
{
    private static final Logger log = Logger.getLogger(TestProcessor.class);
    
    /**
     * Test outer-union.
     * 
     * @throws Exception
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testOuterUnion() throws Exception
    {
        verify("TestProcessor-testOuterUnion", true);
    }
    
    /**
     * Verifies the output of the query processor.
     * 
     * @param relative_file_name
     *            the relative path of the XML file specifying the test case.
     * @param check_output
     *            whether to check the output of the query.
     * @throws Exception
     *             if an error occurs.
     */
    protected void verify(String relative_file_name, boolean check_output) throws Exception
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
            
            log.info(physical_plan.getLogicalPlan().toExplainString());
            
            ConsistencyChecker.getInstance(uas).check(physical_plan.getLogicalPlan());
            DistributedNormalFormChecker.getInstance(uas).check(physical_plan.getLogicalPlan());
            
            if (check_output)
            {
                checkPlanEvaluation(physical_plan, uas);
            }
            else
            {
                Value result = qp.createEagerQueryResult(physical_plan, uas).getValue();
                Document actual_doc = createDocument();
                ValueXmlSerializer.serializeValue(result, "root", actual_doc);
                log.info(XmlUtil.serializeDomToString(actual_doc, true, true));
            }
        }
    }
    
}
