/**
 * 
 */
package edu.ucsd.forward.query.logical;

import static edu.ucsd.forward.xml.XmlUtil.createDocument;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceConfigException;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.DataSourceXmlParser;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.xml.TypeXmlParser;
import edu.ucsd.forward.data.xml.TypeXmlSerializer;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryParsingException;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.xml.PlanXmlParser;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.xml.XmlParserException;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * @author Kevin Zhao
 * 
 */
@Test(groups = AbstractTestCase.FAILURE)
public class TestPrimaryKeyInference extends AbstractTestCase
{
    private static final Logger     log              = Logger.getLogger(TestPrimaryKeyInference.class);
    
    private static final String     DATA_SOURCE_FILE = "TestPrimaryKeyInference-data-source.xml";
    
    private UnifiedApplicationState m_uas;
    
    @BeforeClass
    public void setUp() throws DataSourceConfigException, DataSourceException, QueryExecutionException
    {
        Element test_case_elm = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), DATA_SOURCE_FILE));
        m_uas = new UnifiedApplicationState(DataSourceXmlParser.parse(test_case_elm, new LocationImpl(DATA_SOURCE_FILE)));
        
        m_uas.open();
    }
    
    @AfterClass
    public void cleanUp() throws QueryExecutionException
    {
        m_uas.close();
    }
    
    /**
     * Tests scanning an absolute variable pointing to a collection.
     * 
     * @throws QueryParsingException
     * @throws QueryCompilationException
     * @throws QueryExecutionException
     * @throws XmlParserException
     */
    public void testScan() throws QueryParsingException, QueryCompilationException, QueryExecutionException, XmlParserException
    {
        testRewritingLogicalPlan("TestPrimaryKeyInference-testScan.xml");
    }
    
    /**
     * Tests scanning an (absolute) variable pointing to a tuple does not change primary key.
     * 
     * @throws QueryParsingException
     * @throws QueryCompilationException
     * @throws QueryExecutionException
     * @throws XmlParserException
     */
    public void testScan2() throws QueryParsingException, QueryCompilationException, QueryExecutionException, XmlParserException
    {
        testRewritingLogicalPlan("TestPrimaryKeyInference-testScan2.xml");
    }
    
    /**
     * Tests scan combining two set of keys.
     * 
     * @throws QueryParsingException
     * @throws QueryCompilationException
     * @throws QueryExecutionException
     * @throws XmlParserException
     */
    public void testScan3() throws QueryParsingException, QueryCompilationException, QueryExecutionException, XmlParserException
    {
        testRewritingLogicalPlan("TestPrimaryKeyInference-testScan3.xml");
    }
    
    /**
     * Tests scan type has no key and does not work.
     * 
     * @throws QueryParsingException
     * @throws QueryCompilationException
     * @throws QueryExecutionException
     * @throws XmlParserException
     */
    public void testScan4() throws QueryParsingException, QueryCompilationException, QueryExecutionException, XmlParserException
    {
        testRewritingLogicalPlan("TestPrimaryKeyInference-testScan4.xml");
    }
    
    /**
     * Tests child type has no key and does not work.
     * 
     * @throws QueryParsingException
     * @throws QueryCompilationException
     * @throws QueryExecutionException
     * @throws XmlParserException
     */
    public void testScan5() throws QueryParsingException, QueryCompilationException, QueryExecutionException, XmlParserException
    {
        testRewritingLogicalPlan("TestPrimaryKeyInference-testScan5.xml");
    }
    
    /**
     * Tests select operator propagates the key.
     * 
     * @throws QueryParsingException
     * @throws QueryCompilationException
     * @throws QueryExecutionException
     * @throws XmlParserException
     */
    public void testSelect() throws QueryParsingException, QueryCompilationException, QueryExecutionException, XmlParserException
    {
        testRewritingLogicalPlan("TestPrimaryKeyInference-testSelect.xml");
    }
    
    /**
     * Tests select operator without input key -- no output key either.
     * 
     * @throws QueryParsingException
     * @throws QueryCompilationException
     * @throws QueryExecutionException
     * @throws XmlParserException
     */
    public void testSelect2() throws QueryParsingException, QueryCompilationException, QueryExecutionException, XmlParserException
    {
        testRewritingLogicalPlan("TestPrimaryKeyInference-testSelect2.xml");
    }
    
    /**
     * Tests apply operator propagates the key.
     * 
     * @throws QueryParsingException
     * @throws QueryCompilationException
     * @throws QueryExecutionException
     * @throws XmlParserException
     */
    public void testApply() throws QueryParsingException, QueryCompilationException, QueryExecutionException, XmlParserException
    {
        testRewritingLogicalPlan("TestPrimaryKeyInference-testApply.xml");
    }
    
    /**
     * Tests apply operator without input key -- no output key either.
     * 
     * @throws QueryParsingException
     * @throws QueryCompilationException
     * @throws QueryExecutionException
     * @throws XmlParserException
     */
    public void testApply2() throws QueryParsingException, QueryCompilationException, QueryExecutionException, XmlParserException
    {
        testRewritingLogicalPlan("TestPrimaryKeyInference-testApply2.xml");
    }
    
    /**
     * Tests project - when key is kept.
     * 
     * @throws QueryParsingException
     * @throws QueryCompilationException
     * @throws QueryExecutionException
     * @throws XmlParserException
     */
    public void testProject() throws QueryParsingException, QueryCompilationException, QueryExecutionException, XmlParserException
    {
        testRewritingLogicalPlan("TestPrimaryKeyInference-testProject.xml");
    }
    
    /**
     * Tests project - when key is projected away - no key left.
     * 
     * @throws QueryParsingException
     * @throws QueryCompilationException
     * @throws QueryExecutionException
     * @throws XmlParserException
     */
    public void testProject2() throws QueryParsingException, QueryCompilationException, QueryExecutionException, XmlParserException
    {
        testRewritingLogicalPlan("TestPrimaryKeyInference-testProject2.xml");
    }
    
    private void testRewritingLogicalPlan(String plan_xml_file)
            throws QueryParsingException, QueryCompilationException, QueryExecutionException, XmlParserException
    {
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        qp.setUnifiedApplicationState(m_uas);
        PlanXmlParser pxp = new PlanXmlParser();
        Element root_element = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), plan_xml_file));
        Element plan_element = XmlUtil.getOnlyChildElement(root_element, "query_plan");
        LogicalPlan logical_plan = pxp.parseLogicalPlan(m_uas, plan_element);
        
        Type actual = qp.getOutputType(logical_plan, m_uas);
        printTypeXml(actual);
        
        Type expected = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(root_element, "output_schema")).getRootType();
        assertDeepEqualByIsomorphism(expected, actual);
    }
    
    /**
     * Prints the given actual schema tree in XML.
     * 
     * @param actual_schema
     *            the actual schema tree
     */
    private void printTypeXml(Type actual_type)
    {
        Document result_doc = createDocument();
        TypeXmlSerializer.serializeType(actual_type, "root", result_doc);
        log.debug("\n{}", XmlUtil.serializeDomToString(result_doc, true, true));
    }
}
