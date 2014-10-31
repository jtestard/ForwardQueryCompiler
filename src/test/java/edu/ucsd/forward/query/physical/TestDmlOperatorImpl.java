/**
 * 
 */
package edu.ucsd.forward.query.physical;

import static edu.ucsd.forward.data.xml.ValueXmlSerializer.serializeDataTree;
import static edu.ucsd.forward.xml.XmlUtil.createDocument;
import static edu.ucsd.forward.xml.XmlUtil.serializeDomToString;

import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;

import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.visitors.ConsistencyChecker;
import edu.ucsd.forward.query.logical.visitors.DistributedNormalFormChecker;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.xml.PlanXmlSerializer;

/**
 * Tests physical DML operator implementation.
 * 
 * @author Yupeng
 * @author Romain Vernoux
 */
@Test
public class TestDmlOperatorImpl extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger  log          = Logger.getLogger(TestDmlOperatorImpl.class);
    
    /**
     * If this flag is enabled, the testcases log information useful for creating/debugging test cases.
     */
    private static final boolean DEBUG        = false;
    
    private static final String  EXPECTED_ELM = "expected";
    
    private static final String  ACTUAL_ELM   = "actual";
    
    /**
     * Tests the implementation of insert.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testInsert() throws Exception
    {
        checkOperatorImpl("TestDmlOperatorImpl-testInsertFlatSubQuery");
        checkOperatorImpl("TestDmlOperatorImpl-testInsertFlatTuple");
        checkOperatorImpl("TestDmlOperatorImpl-testInsertFlatWithColumnList");
        checkOperatorImpl("TestDmlOperatorImpl-testInsertNestedAndSubQuery");
        checkOperatorImpl("TestDmlOperatorImpl-testInsertFlatJdbc");
        checkOperatorImpl("TestDmlOperatorImpl-testInsertFlatWithColumnListJdbc");
    }
    
    /**
     * 
     * Tests the implementation of delete.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testDelete() throws Exception
    {
        checkOperatorImpl("TestDmlOperatorImpl-testDeleteFlat");
        checkOperatorImpl("TestDmlOperatorImpl-testDeleteFlatEmpty");
        checkOperatorImpl("TestDmlOperatorImpl-testDeleteNested");
        checkOperatorImpl("TestDmlOperatorImpl-testDeleteFlatSubQuery");
        checkOperatorImpl("TestDmlOperatorImpl-testDeleteFlatSubQueryJdbc");
        checkOperatorImpl("TestDmlOperatorImpl-testDeleteFlatJdbc");
        checkOperatorImpl("TestDmlOperatorImpl-testDeleteFlatJdbcSubQueryJdbc");
    }
    
    /**
     * 
     * Tests the implementation of update.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testUpdate() throws Exception
    {
        checkOperatorImpl("TestDmlOperatorImpl-testUpdateRootScalar");
        checkOperatorImpl("TestDmlOperatorImpl-testUpdateRootTuple");
        checkOperatorImpl("TestDmlOperatorImpl-testUpdateRootTupleCoercion");
        checkOperatorImpl("TestDmlOperatorImpl-testUpdateRootTupleAttribute");
        checkOperatorImpl("TestDmlOperatorImpl-testUpdateRootCollection");
        checkOperatorImpl("TestDmlOperatorImpl-testUpdateCollection");
        checkOperatorImpl("TestDmlOperatorImpl-testUpdateFlat");
        checkOperatorImpl("TestDmlOperatorImpl-testUpdateNested");
        checkOperatorImpl("TestDmlOperatorImpl-testUpdateFlatJdbc");
        checkOperatorImpl("TestDmlOperatorImpl-testUpdateFlatJdbcSubQuery");
    }
    
    /**
     * Tests the output of the physical plan meets the expectation.
     * 
     * @param relative_file_name
     *            the relative path of the XML file specifying the test case.
     * @throws Exception
     *             if an error occurs.
     */
    private void checkOperatorImpl(String relative_file_name) throws Exception
    {
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), relative_file_name + ".xml");
        
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        // Parse the query
        String query_expr = getQueryExpression(0);
        List<AstTree> ast_trees = qp.parseQuery(query_expr, new LocationImpl(relative_file_name + ".xml"));
        assert (ast_trees.size() == 1);
        
        // Translate the AST
        LogicalPlan actual_input = qp.translate(Collections.singletonList(ast_trees.get(0)), uas).get(0);
        actual_input = qp.distribute(Collections.singletonList(actual_input), uas).get(0);
        
        if (DEBUG)
        {
            logLogicalPlanExplain(actual_input);
            logLogicalPlanXml(actual_input);
        }
        
        LogicalPlan expected_input = getLogicalPlan(0);
        checkLogicalPlan(actual_input, expected_input, uas);
        
        ConsistencyChecker.getInstance(uas).check(expected_input);
        DistributedNormalFormChecker.getInstance(uas).check(expected_input);
        
        // Check that the logical plan can be converted to physical plan
        PhysicalPlan actual = qp.generate(Collections.singletonList(expected_input), uas).get(0);
        
        if (DEBUG)
        {
            logLogicalPlanExplain(actual.getLogicalPlan());
            logPhysicalPlanXml(actual);
        }
        
        PhysicalPlan expected = getPhysicalPlan(0);
        
        // Check the logical to physical plan translation
        checkPhysicalPlan(actual, expected);
        
        // Check if the output of the physical plan is evaluated correctly
        checkPlanEvaluation(expected, uas);
        
        // Check if the DML execution result is correct
        DataTree expected_output = null;
        try
        {
            expected_output = uas.getDataSource(EXPECTED_ELM).getDataObject(EXPECTED_ELM);
        }
        catch (DataSourceException e)
        {
            // This should never happen
            throw new AssertionError(e);
        }
        DataTree actual_output = null;
        try
        {
            actual_output = uas.getDataSource(ACTUAL_ELM).getDataObject(ACTUAL_ELM);
        }
        catch (DataSourceException e)
        {
            // This should never happen
            throw new AssertionError(e);
        }
        
        Document actual_doc = createDocument();
        serializeDataTree(actual_output, actual_doc);
        
        Document expected_doc = createDocument();
        serializeDataTree(expected_output, expected_doc);
        
        // Check that the XML trees are isomorphic
        assertEquals(serializeDomToString(expected_doc, true, true), serializeDomToString(actual_doc, true, true));
        
        // Check that the XML plan serializer and parser work for this operator implementation
        PlanXmlSerializer plan_serializer = new PlanXmlSerializer();
        Element element = plan_serializer.serializePhysicalPlan(expected);
        expected = parsePhysicalPlan(element, uas, new LocationImpl(relative_file_name));
    }
    
}
