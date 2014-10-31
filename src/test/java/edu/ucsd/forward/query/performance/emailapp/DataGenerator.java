/**
 * 
 */
package edu.ucsd.forward.query.performance.emailapp;

import static edu.ucsd.forward.xml.XmlUtil.createDocument;
import static edu.ucsd.forward.xml.XmlUtil.serializeDomToString;

import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.data.xml.TypeXmlSerializer;
import edu.ucsd.forward.data.xml.ValueXmlSerializer;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * Generates data for the performance test.
 * 
 * @author Yupeng
 * 
 */
@Test(groups = AbstractTestCase.PERFORMANCE)
public class DataGenerator extends AbstractQueryTestCase
{
    private static final Logger log        = Logger.getLogger(DataGenerator.class);
    
    private static final String DATAOBJECT = "data_object";
    
    private Document            m_document = createDocument();
    
    /**
     * Generates an instance of the data.
     * 
     * @throws Exception
     *             if an error occurs.
     * 
     */
    public void testGenerate() throws Exception
    {
        generate("taylor@enron.com");
    }
    
    /**
     * Generates the data related to a particular user id.
     * 
     * @param user
     *            the user whose related data to be generated.
     * @throws Exception
     *             if an error occurs.
     */
    private void generate(String user) throws Exception
    {
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), "DataGenerator-source.xml");
        
        Element test_case_elm = m_document.createElement("test_case");
        m_document.appendChild(test_case_elm);
        
        // Generate the employees
        String query = "select * from mem_src.employees as e";
        runQuery(query, test_case_elm, "employees");
        
        // Generate the messages of the user
        query = "(select m.* from public.message as m, (select r.mid as mid from public.recipientinfo as r " + "where rvalue = '" + user
                + "' group by r.mid) as t where m.mid = t.mid )" + "union (select * from public.message as m where m.sender= '"
                + user + "')";
        runQuery(query, test_case_elm, "messages");
        
        // Generate the recipient information of the user
//        query = "select * from public.recipientinfo where rvalue = '" + user + "'";
//        runQuery(query, test_case_elm, "recipions");
        
        IoUtil.writeToFile("data.xml", serializeDomToString(m_document, true, true));
    }
    
    /**
     * Run a query.
     * 
     * @param query
     *            the query to run
     * @param test_case_elm
     *            the test case element
     * @param name
     *            the name of the result.
     * @throws Exception
     *             if an error occurs.
     */
    private void runQuery(String query, Element test_case_elm, String name) throws Exception
    {
        Element element = m_document.createElement(DATAOBJECT);
        test_case_elm.appendChild(element);
        element.setAttribute("name", name);
        element.setAttribute("execution_data_source", "mem_src");
        
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        List<AstTree> ast_trees;
        
        ast_trees = qp.parseQuery(query, new LocationImpl(query));
        AstTree ast_tree = ast_trees.get(0);
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        PhysicalPlan physical_plan = qp.compile(Collections.singletonList(ast_tree), uas).get(0);
        
        SchemaTree schema_tree = new SchemaTree(qp.getOutputType(physical_plan.getLogicalPlan(), uas));
        TypeXmlSerializer.serializeSchemaTree(schema_tree, element);
        
        Value result = qp.createEagerQueryResult(physical_plan, uas).getValue();
        ValueXmlSerializer.serializeDataTree(new DataTree(result), element);
        
        log.info(name + " generated.");
        
    }
}
