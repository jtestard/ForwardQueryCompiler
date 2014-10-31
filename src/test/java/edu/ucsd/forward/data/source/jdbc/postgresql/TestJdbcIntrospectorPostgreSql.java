/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.postgresql;

import static edu.ucsd.forward.xml.XmlUtil.createDocument;

import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;

import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceTransaction;
import edu.ucsd.forward.data.source.SchemaObject;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.source.jdbc.JdbcDataSource;
import edu.ucsd.forward.data.source.jdbc.model.Table;
import edu.ucsd.forward.data.source.jdbc.model.Table.TableScope;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.data.xml.TypeXmlSerializer;
import edu.ucsd.forward.data.xml.ValueXmlSerializer;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.function.FunctionRegistryException;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.external.ExternalFunction;
import edu.ucsd.forward.query.logical.visitors.ConsistencyChecker;
import edu.ucsd.forward.query.logical.visitors.DistributedNormalFormChecker;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Tests the JDBC data source introspector.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@Test
public class TestJdbcIntrospectorPostgreSql extends AbstractQueryTestCase
{
    private static final Logger log             = Logger.getLogger(TestJdbcIntrospectorPostgreSql.class);
    
    private static final String FUNCTIONS_SETUP = "TestJdbcIntrospectorPostgreSql-testFunctionsSetup8_4";
    
    private static final String CLEANUP_FILE    = "TestJdbcIntrospectorPostgreSql-testCleanup";
    
    /**
     * Tests the PostgreSQL JDBC data source introspector.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testSimple() throws Exception
    {
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), "TestJdbcIntrospectorPostgreSql-testSimple.xml");
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        JdbcDataSource jdbc_data_source = (JdbcDataSource) uas.getDataSource("jdbc");
        
        for (SchemaObject object : jdbc_data_source.getSchemaObjects())
        {
            String name = object.getName();
            SchemaObject expected_object = jdbc_data_source.getSchemaObject(name);
            SchemaTree expected_schema_tree = expected_object.getSchemaTree();
            SchemaTree actual_schema_tree = object.getSchemaTree();
            assertTrue(TypeUtil.deepEqualsByIsomorphism(expected_schema_tree, actual_schema_tree));
            Table.create(TableScope.PERSISTENT, jdbc_data_source.getMetaData().getSchemaName(), object.getName(),
                         (CollectionType) object.getSchemaTree().getRootType());
            
            DataSourceTransaction transaction = jdbc_data_source.beginTransaction();
            
            jdbc_data_source.getDataObject(name, transaction);
            
            transaction.commit();
        }
    }
    
    /**
     * Tests the PostgreSQL JDBC data source introspector.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testTypes() throws Exception
    {
        JdbcDataSource data_source = null;
        try
        {
            data_source = setupJdbcDataSource(this.getClass(), "TestJdbcIntrospectorPostgreSql-testTypesSetup", CLEANUP_FILE);
            
            // Parse the test case from the XML file
            parseTestCase(this.getClass(), "TestJdbcIntrospectorPostgreSql-testTypes.xml");
            
            UnifiedApplicationState uas = getUnifiedApplicationState();
            
            JdbcDataSource jdbc_data_source = (JdbcDataSource) uas.getDataSource("jdbc");
            DataSource mediator = uas.getDataSource(DataSource.MEDIATOR);
            
            String test_case_str = IoUtil.getResourceAsString(this.getClass(), "TestJdbcIntrospectorPostgreSql-testTypes.xml");
            Element test_case_elm = (Element) XmlUtil.parseDomNode(test_case_str);
            Element output = XmlUtil.getOnlyChildElement(test_case_elm, "output");
            
            String expected = XmlUtil.serializeDomToString(XmlUtil.getChildElements(output, "schema_tree").get(0), true, true);
            Document result_doc = createDocument();
            TypeXmlSerializer.serializeSchemaTree(jdbc_data_source.getSchemaObject("proposals").getSchemaTree(), result_doc);
            String actual = XmlUtil.serializeDomToString(result_doc, true, true);
            assertEquals(expected, actual);
            
            expected = XmlUtil.serializeDomToString(XmlUtil.getChildElements(output, "schema_tree").get(1), true, true);
            result_doc = createDocument();
            TypeXmlSerializer.serializeSchemaTree(jdbc_data_source.getSchemaObject("reviewers").getSchemaTree(), result_doc);
            actual = XmlUtil.serializeDomToString(result_doc, true, true);
            assertEquals(expected, actual);
            
            expected = XmlUtil.serializeDomToString(XmlUtil.getChildElements(output, "schema_tree").get(2), true, true);
            result_doc = createDocument();
            TypeXmlSerializer.serializeSchemaTree(jdbc_data_source.getSchemaObject("assignments").getSchemaTree(), result_doc);
            actual = XmlUtil.serializeDomToString(result_doc, true, true);
            assertEquals(expected, actual);
            
            expected = XmlUtil.serializeDomToString(XmlUtil.getChildElements(output, "schema_tree").get(3), true, true);
            result_doc = createDocument();
            TypeXmlSerializer.serializeSchemaTree(mediator.getSchemaObject("faculty").getSchemaTree(), result_doc);
            actual = XmlUtil.serializeDomToString(result_doc, true, true);
            assertEquals(expected, actual);
        }
        catch (Exception e)
        {
            throw e;
        }
        
        finally
        {
            if (data_source != null) cleanupJdbcDataSource(this.getClass(), data_source, CLEANUP_FILE);
        }
    }
    
    /**
     * Tests setting up primary key and foreign key constraints.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testConstraints() throws Exception
    {
        JdbcDataSource data_source = null;
        try
        {
            data_source = setupJdbcDataSource(this.getClass(), "TestJdbcIntrospectorPostgreSql-testConstraintsSetup", CLEANUP_FILE);
            
            // Parse the test case from the XML file
            parseTestCase(this.getClass(), "TestJdbcIntrospectorPostgreSql-testConstraints.xml");
            
            UnifiedApplicationState uas = getUnifiedApplicationState();
            
            JdbcDataSource jdbc_data_source = (JdbcDataSource) uas.getDataSource("jdbc");
            
            String test_case_str = IoUtil.getResourceAsString(this.getClass(), "TestJdbcIntrospectorPostgreSql-testConstraints.xml");
            Element test_case_elm = (Element) XmlUtil.parseDomNode(test_case_str);
            Element output = XmlUtil.getOnlyChildElement(test_case_elm, "output");
            
            String expected = XmlUtil.serializeDomToString(XmlUtil.getChildElements(output, "schema_tree").get(0), true, true);
            Document result_doc = createDocument();
            TypeXmlSerializer.serializeSchemaTree(jdbc_data_source.getSchemaObject("supported_pkey").getSchemaTree(), result_doc);
            String actual = XmlUtil.serializeDomToString(result_doc, true, true);
            assertEquals(expected, actual);
            
            expected = XmlUtil.serializeDomToString(XmlUtil.getChildElements(output, "schema_tree").get(1), true, true);
            result_doc = createDocument();
            TypeXmlSerializer.serializeSchemaTree(jdbc_data_source.getSchemaObject("unsupported_pkey").getSchemaTree(), result_doc);
            actual = XmlUtil.serializeDomToString(result_doc, true, true);
            assertEquals(expected, actual);
            
            expected = XmlUtil.serializeDomToString(XmlUtil.getChildElements(output, "schema_tree").get(2), true, true);
            result_doc = createDocument();
            TypeXmlSerializer.serializeSchemaTree(jdbc_data_source.getSchemaObject("supported_fkey").getSchemaTree(), result_doc);
            actual = XmlUtil.serializeDomToString(result_doc, true, true);
            assertEquals(expected, actual);
            
            expected = XmlUtil.serializeDomToString(XmlUtil.getChildElements(output, "schema_tree").get(3), true, true);
            result_doc = createDocument();
            TypeXmlSerializer.serializeSchemaTree(jdbc_data_source.getSchemaObject("unsupported_fkey").getSchemaTree(), result_doc);
            actual = XmlUtil.serializeDomToString(result_doc, true, true);
            assertEquals(expected, actual);
            
            expected = XmlUtil.serializeDomToString(XmlUtil.getChildElements(output, "schema_tree").get(4), true, true);
            result_doc = createDocument();
            TypeXmlSerializer.serializeSchemaTree(jdbc_data_source.getSchemaObject("non_null_table").getSchemaTree(), result_doc);
            actual = XmlUtil.serializeDomToString(result_doc, true, true);
            assertEquals(expected, actual);
        }
        finally
        {
            if (data_source != null) cleanupJdbcDataSource(this.getClass(), data_source, CLEANUP_FILE);
        }
    }
    
    /**
     * Tests the PostgreSQL JDBC data source introspector.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testTypes8_4() throws Exception
    {
        JdbcDataSource data_source = null;
        try
        {
            data_source = setupJdbcDataSource(this.getClass(), "TestJdbcIntrospectorPostgreSql-testTypesSetup8_4", CLEANUP_FILE);
            
            // Parse the test case from the XML file
            parseTestCase(this.getClass(), "TestJdbcIntrospectorPostgreSql-testTypes8_4.xml");
            
            UnifiedApplicationState uas = getUnifiedApplicationState();
            
            JdbcDataSource jdbc_data_source = (JdbcDataSource) uas.getDataSource("jdbc");
            
            String test_case_str = IoUtil.getResourceAsString(this.getClass(), "TestJdbcIntrospectorPostgreSql-testTypes8_4.xml");
            Element test_case_elm = (Element) XmlUtil.parseDomNode(test_case_str);
            Element output = XmlUtil.getOnlyChildElement(test_case_elm, "output");
            
            String expected = XmlUtil.serializeDomToString(XmlUtil.getChildElements(output, "schema_tree").get(0), true, true);
            Document result_doc = createDocument();
            TypeXmlSerializer.serializeSchemaTree(jdbc_data_source.getSchemaObject("supported_types").getSchemaTree(), result_doc);
            String actual = XmlUtil.serializeDomToString(result_doc, true, true);
            assertEquals(expected, actual);
            
            expected = XmlUtil.serializeDomToString(XmlUtil.getChildElements(output, "schema_tree").get(1), true, true);
            result_doc = createDocument();
            TypeXmlSerializer.serializeSchemaTree(jdbc_data_source.getSchemaObject("unsupported_types").getSchemaTree(),
                                                  result_doc);
            actual = XmlUtil.serializeDomToString(result_doc, true, true);
            assertEquals(expected, actual);
            
            // FIXME : Time not supported?
        }
        finally
        {
            if (data_source != null) cleanupJdbcDataSource(this.getClass(), data_source, CLEANUP_FILE);
        }
    }
    
    /**
     * Tests functions that a return single column and only a single row.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testFunctionsSingleColumnRow() throws Exception
    {
        JdbcDataSource data_source = null;
        try
        {
            data_source = setupJdbcDataSource(this.getClass(), FUNCTIONS_SETUP, CLEANUP_FILE);
            
            // Parse the test case from the XML file
            parseTestCase(this.getClass(), "TestJdbcIntrospectorPostgreSql-testFunctions8_4.xml");
            
            UnifiedApplicationState uas = getUnifiedApplicationState();
            
            JdbcDataSource jdbc_data_source = (JdbcDataSource) uas.getDataSource("jdbc");
            
            /* Returns single column result, single row */
            { // Check add function
                ExternalFunction ef = jdbc_data_source.getExternalFunction("add");
                FunctionSignature sig = ef.getFunctionSignatures().get(0);
                assertTrue(sig.getReturnType() instanceof IntegerType);
                assertTrue(sig.getArguments().get(0).getType() instanceof IntegerType);
                assertTrue(sig.getArguments().get(1).getType() instanceof IntegerType);
            }
            
            { // Check double function
                ExternalFunction ef = jdbc_data_source.getExternalFunction("dble");
                FunctionSignature sig = ef.getFunctionSignatures().get(0);
                assertTrue(sig.getReturnType() instanceof IntegerType);
                assertTrue(sig.getArguments().size() == 1);
                assertTrue(sig.getArguments().get(0).getType() instanceof IntegerType);
            }
            
            { // Check getValue1 function (inferred output)
                ExternalFunction ef = jdbc_data_source.getExternalFunction("getValue1");
                FunctionSignature sig = ef.getFunctionSignatures().get(0);
                assertTrue(sig.getReturnType() instanceof IntegerType);
                assertTrue(sig.getArguments().size() == 0);
            }
            
            { // Check getValue2 function (explicit return)
                ExternalFunction ef = jdbc_data_source.getExternalFunction("getValue2");
                FunctionSignature sig = ef.getFunctionSignatures().get(0);
                assertTrue(sig.getReturnType() instanceof IntegerType);
                assertTrue(sig.getArguments().size() == 0);
            }
            
            { // Check getValue3 function (inferred output)
                ExternalFunction ef = jdbc_data_source.getExternalFunction("getValue3");
                FunctionSignature sig = ef.getFunctionSignatures().get(0);
                assertTrue(sig.getReturnType() instanceof IntegerType);
                assertTrue(sig.getArguments().size() == 1);
                assertTrue(sig.getArguments().get(0).getType() instanceof IntegerType);
            }
            
            { // Check getValue4 function (explicit return)
                ExternalFunction ef = jdbc_data_source.getExternalFunction("getValue4");
                FunctionSignature sig = ef.getFunctionSignatures().get(0);
                assertTrue(sig.getReturnType() instanceof IntegerType);
                assertTrue(sig.getArguments().size() == 1);
                assertTrue(sig.getArguments().get(0).getType() instanceof IntegerType);
            }
        }
        finally
        {
            if (data_source != null) cleanupJdbcDataSource(this.getClass(), data_source, CLEANUP_FILE);
        }
    }
    
    /**
     * Tests functions that return a single column, but multiple rows.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testSingleColumnMultiRow() throws Exception
    {
        JdbcDataSource data_source = null;
        try
        {
            data_source = setupJdbcDataSource(this.getClass(), FUNCTIONS_SETUP, CLEANUP_FILE);
            
            // Parse the test case from the XML file
            parseTestCase(this.getClass(), "TestJdbcIntrospectorPostgreSql-testFunctions8_4.xml");
            
            UnifiedApplicationState uas = getUnifiedApplicationState();
            
            JdbcDataSource jdbc_data_source = (JdbcDataSource) uas.getDataSource("jdbc");
            
            // FIXME : Set returning functions not allowed in new Query Processor
            // See history for old test case when implemented
            testNotImported("getSingleColumnRows1", jdbc_data_source);
            testNotImported("getSingleColumnRows2", jdbc_data_source);
            testNotImported("getSingleColumnRows3", jdbc_data_source);
            testNotImported("getSingleColumnRows4", jdbc_data_source);
            testNotImported("getSingleColumnRows5", jdbc_data_source);
        }
        finally
        {
            if (data_source != null) cleanupJdbcDataSource(this.getClass(), data_source, CLEANUP_FILE);
        }
    }
    
    /**
     * Tests functions that return multiple columns, but only a single row.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testMultiColumnSingleRow() throws Exception
    {
        JdbcDataSource data_source = null;
        try
        {
            data_source = setupJdbcDataSource(this.getClass(), FUNCTIONS_SETUP, CLEANUP_FILE);
            
            // Parse the test case from the XML file
            parseTestCase(this.getClass(), "TestJdbcIntrospectorPostgreSql-testFunctions8_4.xml");
            
            UnifiedApplicationState uas = getUnifiedApplicationState();
            
            JdbcDataSource jdbc_data_source = (JdbcDataSource) uas.getDataSource("jdbc");
            
            // Check dup1 function composite output
            testNotImported("dup1", jdbc_data_source);
            // Check dup2 function using output parameters
            testNotImported("dup2", jdbc_data_source);
            // Check dup2 function using output parameters
            testNotImported("dup3", jdbc_data_source);
            // Check dup4 function mixing inout and out
            testNotImported("dup4", jdbc_data_source);
            // Check variadic function
            testNotImported("stats", jdbc_data_source);
        }
        finally
        {
            if (data_source != null) cleanupJdbcDataSource(this.getClass(), data_source, CLEANUP_FILE);
        }
    }
    
    /**
     * Tests functions that return a single column, but multiple rows.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testMultiColumnMultiRow() throws Exception
    {
        JdbcDataSource data_source = null;
        try
        {
            data_source = setupJdbcDataSource(this.getClass(), FUNCTIONS_SETUP, CLEANUP_FILE);
            
            // Parse the test case from the XML file
            parseTestCase(this.getClass(), "TestJdbcIntrospectorPostgreSql-testFunctions8_4.xml");
            
            UnifiedApplicationState uas = getUnifiedApplicationState();
            
            JdbcDataSource jdbc_data_source = (JdbcDataSource) uas.getDataSource("jdbc");
            
            // Check getMultiColumnRows1 function composite output
            testNotImported("getMultiColumnRows1", jdbc_data_source);
            // Check getMultiColumnRows2 function using output parameters
            testNotImported("getMultiColumnRows2", jdbc_data_source);
            // Check getMultiColumnRows3 function using inout parameters
            testNotImported("getMultiColumnRows3", jdbc_data_source);
            // Check getMultiColumnRows4 function using table
            testNotImported("getMultiColumnRows4", jdbc_data_source);
            // Check getMultiColumnRows4 function using mix of inout and out
            testNotImported("getMultiColumnRows5", jdbc_data_source);
        }
        finally
        {
            if (data_source != null) cleanupJdbcDataSource(this.getClass(), data_source, CLEANUP_FILE);
        }
    }
    
    /**
     * Tests output of functions.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testFunctionOutput() throws Exception
    {
        JdbcDataSource data_source = null;
        try
        {
            data_source = setupJdbcDataSource(this.getClass(), FUNCTIONS_SETUP, CLEANUP_FILE);
            
            // Tests scalar output
            verify("TestJdbcIntrospectorPosgreSql-testFunctionOutput1", true);
            
            // FIXME : Currently, returning setof type in function fails with new query processor 
            // when issuing the following query (select * from func as f)
            // Tests returning sets
            // verify("TestJdbcIntrospectorPosgreSql-testFunctionOutput2", true);
        }
        finally
        {
            if (data_source != null) cleanupJdbcDataSource(this.getClass(), data_source, CLEANUP_FILE);
        }
    }
    
    /**
     * Tests that the given function was not imported.
     * 
     * @param name
     *            the name of the function.
     * @param jdbc_data_source
     *            the jdbc data source that holds the function.
     */
    private void testNotImported(String name, JdbcDataSource jdbc_data_source)
    {
        try
        {
            jdbc_data_source.getExternalFunction(name);
            throw new AssertionError("Function " + name + " should not have been imported");
        }
        catch (FunctionRegistryException e)
        {
            assert (true);
        }
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
