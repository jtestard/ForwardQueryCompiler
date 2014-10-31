/**
 * 
 */
package edu.ucsd.forward.query.defect;

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
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.logical.visitors.ConsistencyChecker;
import edu.ucsd.forward.query.logical.visitors.DistributedNormalFormChecker;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Tests the query processor defects.
 * 
 * @author Erick Zamora
 * 
 */
@Test
public class TestDefects extends AbstractQueryTestCase
{
    private static final Logger log = Logger.getLogger(TestDefects.class);
    
    /**
     * Tests incorrect output type for select * from tuple (x.*).
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testD1() throws Exception
    {
        verify("TestDefect-testD1", true);
    }
    
    /**
     * Tests CASE/WHEN does not support returning tuples.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testD2() throws Exception
    {
        verify("TestDefect-testD2", true);
    }
    
    /**
     * Tests * mixed with other select items.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testD166() throws Exception
    {
        verify("TestDefect-testD166", true);
    }
    
    /**
     * Tests query parsing for decimals where fractional part is 0.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testD206() throws Exception
    {
        verify("TestDefect-testD206", true);
    }
    
    /**
     * PlanToAst fails to translate a Join with a subquery as a child.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testD213() throws Exception
    {
        verify("TestDefect-testD213", true);
    }

    /**
     * Cannot call "select NULL".
     * 
     * @throws Exception .
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testD249() throws Exception
    {
        verify("TestDefect-testD249", true);
    }

    /**
     * Division is sensitive to whitespace.
     * 
     * @throws Exception .
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testD250() throws Exception
    {
        verify("TestDefect-testD250", true);
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
            
            log.debug(physical_plan.getLogicalPlan().toExplainString());
            
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
                log.debug(XmlUtil.serializeDomToString(actual_doc, true, true));
            }
        }
    }
}
