package edu.ucsd.forward.query;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.google.gwt.xml.client.CharacterData;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.mapping.MappingGroup;
import edu.ucsd.forward.data.mapping.MappingXmlParser;
import edu.ucsd.forward.data.mapping.MappingXmlSerializer;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceXmlParser;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.source.jdbc.JdbcDataSource;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.data.xml.TypeXmlSerializer;
import edu.ucsd.forward.data.xml.ValueXmlSerializer;
import edu.ucsd.forward.exception.CheckedException;
import edu.ucsd.forward.exception.ExceptionMessages.QueryCompilation;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.exception.UncheckedException;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.visitors.ConsistencyChecker;
import edu.ucsd.forward.query.logical.visitors.DistributedNormalFormChecker;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.xml.PlanXmlParser;
import edu.ucsd.forward.query.xml.PlanXmlSerializer;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.util.NameGenerator;
import edu.ucsd.forward.util.NameGeneratorFactory;
import edu.ucsd.forward.warning.WarningCollectorFactory;
import edu.ucsd.forward.xml.XmlParserException;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * An abstract implementation of the query processor test cases.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 */
@Test
public class AbstractQueryTestCase extends AbstractTestCase
{
    private static final Logger            log             = Logger.getLogger(AbstractQueryTestCase.class);
    
    protected static final String          QUERY_EXPR_ELM  = "query_expression";
    
    private static final String            QUERY_PLAN_ELM  = "query_plan";
    
    protected static final String          OUTPUT_ELM      = "output";
    
    private static UnifiedApplicationState s_uas;
    
    private static List<String>            s_query_expr    = new ArrayList<String>();
    
    private static List<LogicalPlan>       s_logical_plan  = new ArrayList<LogicalPlan>();
    
    private static List<PhysicalPlan>      s_physical_plan = new ArrayList<PhysicalPlan>();
    
    /**
     * Returns the unified application state.
     * 
     * @return the unified application state.
     */
    public static UnifiedApplicationState getUnifiedApplicationState()
    {
        return s_uas;
    }
    
    /**
     * Sets the unified application state.
     * 
     * @param uas
     *            the unified application state.
     */
    protected static void setUnifiedApplicationState(UnifiedApplicationState uas)
    {
        assert uas != null;
        s_uas = uas;
    }
    
    /**
     * Returns all query expressions.
     * 
     * @return a list of query expressions.
     */
    public static List<String> getQueryExpressions()
    {
        return s_query_expr;
    }
    
    /**
     * Returns the query expression in the given index.
     * 
     * @param index
     *            the index to retrieve.
     * @return a query expression.
     */
    public static String getQueryExpression(int index)
    {
        return s_query_expr.get(index);
    }
    
    /**
     * Returns the logical plan in the given index.
     * 
     * @param index
     *            the index to retrieve.
     * @return a logical plan.
     */
    public static LogicalPlan getLogicalPlan(int index)
    {
        return s_logical_plan.get(index);
    }
    
    /**
     * Returns the physical plan in the given index.
     * 
     * @param index
     *            the index to retrieve.
     * @return a physical plan.
     */
    public static PhysicalPlan getPhysicalPlan(int index)
    {
        return s_physical_plan.get(index);
    }
    
    /**
     * Parses a test case and sets up the test data sources and data objects to be used by a query processing test case.
     * 
     * @param clazz
     *            the calling class to use as the base path to the test case file.
     * @param relative_file_name
     *            the file name of the test case relative to the calling class.
     * @throws CheckedException
     *             if an error occurs.
     */
    protected static void parseTestCase(Class<?> clazz, String relative_file_name) throws CheckedException
    {
        Element element = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(clazz, relative_file_name));
        parseTestCase(element, relative_file_name);
    }
    
    /**
     * Parses a test case and sets up the test data sources and data objects to be used by a query processing test case.
     * 
     * @param test_case_elm
     *            the root element of the test case.
     * @param file_name
     *            the file name of the test case.
     * @throws CheckedException
     *             if an error occurs.
     */
    protected static void parseTestCase(Element test_case_elm, String file_name) throws CheckedException
    {
        // Reset the name counter so that the generated numbers are consistent between test runs
        NameGeneratorFactory.getInstance().reset(NameGenerator.VARIABLE_GENERATOR);
        NameGeneratorFactory.getInstance().reset(NameGenerator.PARAMETER_GENERATOR);
        NameGeneratorFactory.getInstance().reset(NameGenerator.PHYSICAL_PLAN_GENERATOR);
        NameGeneratorFactory.getInstance().reset(NameGenerator.KEY_VARIABLE_GENERATOR);
        NameGeneratorFactory.getInstance().reset(NameGenerator.TEMP_TABLE_GENERATOR);
        NameGeneratorFactory.getInstance().reset(NameGenerator.FUNCTION_PLAN_GENERATOR);
        NameGeneratorFactory.getInstance().reset(NameGenerator.DISTRIBUTOR_VAR_GENERATOR);
        NameGeneratorFactory.getInstance().reset(NameGenerator.PUSH_DOWN_SELECTIONS_GENERATOR);
        NameGeneratorFactory.getInstance().reset(NameGenerator.SQL_SOURCE_WRAPPER_GENERATOR);
        
        s_query_expr.clear();
        s_logical_plan.clear();
        s_physical_plan.clear();
        
        // Close previous unified application state and remove external functions
        // This is necessary because we execute multiple test cases per test method
        if (s_uas != null)
        {
            after();
        }
        
        // Build input data sources and schema and data objects
        s_uas = new UnifiedApplicationState(Collections.<DataSource> emptyList());
        for (DataSource data_source : DataSourceXmlParser.parse(test_case_elm, new LocationImpl(file_name)))
        {
            s_uas.addDataSource(data_source);
        }
        
        // Parse mapping groups
        for (Element mapping_group_elm : XmlUtil.getChildElements(test_case_elm, MappingXmlSerializer.MAPPING_GROUP_ELM))
        {
            MappingGroup group = MappingXmlParser.parseMappingGroup(mapping_group_elm);
            s_uas.addMappingGroup(group);
        }
        
        // Open the unified application state
        s_uas.open();
        
        parseQueryExpressions(test_case_elm);
        
        // Parse the query plans
        List<Element> query_plan_elms = XmlUtil.getChildElements(test_case_elm, QUERY_PLAN_ELM);
        for (Element query_plan_elm : query_plan_elms)
        {
            LogicalPlan logical_plan = parseLogicalPlan(query_plan_elm, s_uas, new LocationImpl(file_name));
            ConsistencyChecker.getInstance(s_uas).check(logical_plan);
            s_logical_plan.add(logical_plan);
            
            PhysicalPlan physical_plan = parsePhysicalPlan(query_plan_elm, s_uas, new LocationImpl(file_name));
            if (physical_plan != null)
            {
                s_physical_plan.add(physical_plan);
                DistributedNormalFormChecker.getInstance(s_uas).check(physical_plan.getLogicalPlan());
            }
        }
    }
    
    /**
     * Parses the query expressions.
     * 
     * @param test_case_elm
     *            the test case element.
     */
    protected static void parseQueryExpressions(Element test_case_elm)
    {
        s_query_expr.clear();
        // Parse the query expressions
        List<Element> query_expr_elms = XmlUtil.getChildElements(test_case_elm, QUERY_EXPR_ELM);
        for (Element query_expr_elm : query_expr_elms)
        {
            s_query_expr.add(((CharacterData) query_expr_elm.getChildNodes().item(0)).getData());
        }
    }
    
    /**
     * Parses an XML representation of a logical plan.
     * 
     * @param query_plan_elm
     *            the query plan element of the XML DOM.
     * @param uas
     *            the unified application state that may be used by the plan.
     * @param location
     *            the location of the query plan.
     * @return the built logical query plan.
     * @throws QueryCompilationException
     *             if an error occurs during query plan parsing.
     */
    public static LogicalPlan parseLogicalPlan(Element query_plan_elm, UnifiedApplicationState uas, Location location)
            throws QueryCompilationException
    {
        try
        {
            LogicalPlan logical = null;
            PlanXmlParser query_plan_parser = new PlanXmlParser();
            try
            {
                logical = query_plan_parser.parseLogicalPlan(uas, query_plan_elm);
            }
            catch (XmlParserException ex)
            {
                // Chain the exception
                throw new QueryCompilationException(QueryCompilation.LOGICAL_PLAN_PARSE, location, ex);
            }
            
            return logical;
        }
        catch (Throwable t)
        {
            // Propagate abnormal conditions immediately
            if (t instanceof java.lang.Error && !(t instanceof AssertionError)) throw (java.lang.Error) t;
            
            // Throw the expected exception
            if (t instanceof QueryCompilationException) throw (QueryCompilationException) t;
            
            // Propagate the unexpected exception
            throw (t instanceof UncheckedException) ? (UncheckedException) t : new UncheckedException(t);
        }
    }
    
    /**
     * Parses an XML representation of a physical plan.
     * 
     * @param query_plan_elm
     *            the query plan element of the XML DOM.
     * @param uas
     *            the unified application state that may be used by the plan.
     * @param location
     *            the location of the query plan.
     * @return the built logical query plan.
     * @throws QueryCompilationException
     *             if an error occurs during query plan parsing.
     */
    public static PhysicalPlan parsePhysicalPlan(Element query_plan_elm, UnifiedApplicationState uas, Location location)
            throws QueryCompilationException
    {
        try
        {
            PhysicalPlan physical = null;
            PlanXmlParser query_plan_parser = new PlanXmlParser();
            try
            {
                physical = query_plan_parser.parsePhysicalPlan(uas, query_plan_elm);
            }
            catch (XmlParserException ex)
            {
                // Chain the exception
                throw new QueryCompilationException(QueryCompilation.PHYSICAL_PLAN_PARSE, location, ex);
            }
            
            return physical;
        }
        catch (Throwable t)
        {
            // Propagate abnormal conditions immediately
            if (t instanceof java.lang.Error && !(t instanceof AssertionError)) throw (java.lang.Error) t;
            
            // Throw the expected exception
            if (t instanceof QueryCompilationException) throw (QueryCompilationException) t;
            
            // Propagate the unexpected exception
            throw (t instanceof UncheckedException) ? (UncheckedException) t : new UncheckedException(t);
        }
    }
    
    /**
     * Checks if the actual logical plan is the expected one.
     * 
     * @param actual
     *            the actual logical plan.
     * @param expected
     *            the expected logical plan.
     * @param uas
     *            the unified application state.
     */
    protected void checkLogicalPlan(LogicalPlan actual, LogicalPlan expected, UnifiedApplicationState uas)
    {
        // FIXME This should compare the logical plans, not their XML representation
        PlanXmlSerializer serializer = new PlanXmlSerializer();
        
        Element actual_elm = serializer.serializeLogicalPlan(actual);
        String actual_str = XmlUtil.serializeDomToString(actual_elm, true, true);
        
        Element expected_elm = serializer.serializeLogicalPlan(expected);
        String expected_str = XmlUtil.serializeDomToString(expected_elm, true, true);
        
        assertEquals(expected_str, actual_str);
    }
    
    /**
     * Checks if the actual physical plan is the expected one.
     * 
     * @param actual
     *            the actual physical plan.
     * @param expected
     *            the expected physical plan.
     */
    protected void checkPhysicalPlan(PhysicalPlan actual, PhysicalPlan expected)
    {
        // FIXME This should compare the physical plans, not their XML representation
        PlanXmlSerializer serializer = new PlanXmlSerializer();
        
        Element actual_elm = serializer.serializePhysicalPlan(actual);
        String actual_str = XmlUtil.serializeDomToString(actual_elm, true, true);
        
        Element expected_elm = serializer.serializePhysicalPlan(expected);
        String expected_str = XmlUtil.serializeDomToString(expected_elm, true, true);
        
        assertEquals(expected_str, actual_str);
    }
    
    /**
     * Evaluates the physical plan and and checks if the output is the same as expected.
     * 
     * @param physical_plan
     *            the physical plan to be evaluated
     * @param uas
     *            the unified application state that contains the expected output.
     * @return the actual returned value.
     * @throws CheckedException
     *             if an error occurs.
     */
    protected static Value checkPlanEvaluation(PhysicalPlan physical_plan, UnifiedApplicationState uas) throws CheckedException
    {
        // Execute the plan
        Value query_result = QueryProcessorFactory.getInstance().createEagerQueryResult(physical_plan, uas).getValue();
        
        checkOutputValue(query_result, uas);
        
        return query_result;
    }
    
    /**
     * Checks if the given output value is the same as expected.
     * 
     * @param value
     *            the output value
     * @param uas
     *            the unified application state that contains the expected output.
     * @throws CheckedException
     *             if an error occurs.
     */
    protected static void checkOutputValue(Value value, UnifiedApplicationState uas) throws CheckedException
    {
        DataTree actual_output = new DataTree(value);
        
        DataTree expected_output = uas.getDataSource(OUTPUT_ELM).getDataObject(OUTPUT_ELM);
        
        // FIXME This should compare the values, not their XML representation
        AbstractTestCase.assertEqualSerialization(expected_output.getRootValue(), actual_output.getRootValue());
        
        // FIXME Check that the schema trees are isomorphic
    }
    
    /**
     * Sets up and returns a JDBC data source.
     * 
     * @param clazz
     *            the calling class.
     * @param relative_filename
     *            the filename to use for data source configuration (.xml) and setup SQL script (.sql) to execute.
     * @param relative_filename2
     *            the filename to use for cleanup SQL script (.sql) to execute.
     * @return a JDBC data source.
     * @throws Exception
     *             if something goes wrong.
     */
    protected JdbcDataSource setupJdbcDataSource(Class<? extends AbstractQueryTestCase> clazz, String relative_filename,
            String relative_filename2) throws Exception
    {
        JdbcDataSource data_source = null;
        Connection connection = null;
        Statement stmt = null;
        
        // Parse the data source configuration file
        Element root_element = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(clazz, relative_filename + ".xml"));
        
        // Find JDBC data source in the configuration file
        Collection<DataSource> data_sources = DataSourceXmlParser.parse(root_element, new LocationImpl(relative_filename + ".xml"));
        
        // Get the public data source
        data_source = (JdbcDataSource) data_sources.iterator().next();
        data_source.open();
        connection = data_source.getConnectionPool().getConnection();
        stmt = connection.createStatement();
        
        // Clean up the external functions if exists
        stmt.execute(IoUtil.getResourceAsString(clazz, relative_filename2 + ".sql"));
        
        // Setup the external functions
        stmt.execute(IoUtil.getResourceAsString(clazz, relative_filename + ".sql"));
        
        connection.commit();
        connection.close();
        data_source.close();
        
        return data_source;
    }
    
    /**
     * Cleans up a JDBC data source.
     * 
     * @param clazz
     *            the calling class.
     * @param data_source
     *            the JDBC data source to cleanup.
     * @param relative_filename
     *            the filename to use for cleanup SQL script (.sql) to execute.
     * @throws Exception
     *             if something goes wrong.
     */
    protected void cleanupJdbcDataSource(Class<? extends AbstractQueryTestCase> clazz, JdbcDataSource data_source,
            String relative_filename) throws Exception
    {
        data_source.open();
        Connection connection = data_source.getConnectionPool().getConnection();
        Statement stmt = connection.createStatement();
        
        // Clean up the external functions
        stmt.execute(IoUtil.getResourceAsString(clazz, relative_filename + ".sql"));
        
        connection.commit();
        connection.close();
        data_source.close();
    }
    
    /**
     * Closes the unified application state after each test is performed.
     * 
     * @throws CheckedException
     *             if an exception occurs.
     */
    @AfterMethod
    public static void after() throws CheckedException
    {
        // First cleanup the query processor so that DataSourceAccesses are removed along with active transactions
        QueryProcessorFactory.getInstance().cleanup(true);
        
        // Then close the UAS
        if (s_uas != null)
        {
            s_uas.close();
        }
        
        // Clear the warnings
        WarningCollectorFactory.getInstance().clear();
    }
    
    /**
     * Explains a query expression and returns the physical plan in XML format of string.
     * 
     * @param query_expr
     *            the query to explain
     * @return the explain of the query
     * @throws Exception
     *             if something goes wrong
     */
    protected String explainQuery(String query_expr) throws Exception
    {
        NameGeneratorFactory.getInstance().resetAll();
        UnifiedApplicationState uas = getUnifiedApplicationState();
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        
        List<AstTree> ast_trees = qp.parseQuery(query_expr, new LocationImpl());
        assert (ast_trees.size() == 1);
        
        LogicalPlan actual = qp.translate(Collections.singletonList(ast_trees.get(0)), uas).get(0);
        
        LogicalPlan rewritten = qp.rewriteSourceAgnostic(Collections.singletonList(actual), uas).get(0);
        LogicalPlan distributed = qp.distribute(Collections.singletonList(rewritten), uas).get(0);
        PhysicalPlan physical = qp.generate(Collections.singletonList(distributed), uas).get(0);
        
        PlanXmlSerializer serializer = new PlanXmlSerializer();
        
        Element actual_elm = serializer.serializePhysicalPlan(physical);
        String actual_str = XmlUtil.serializeDomToString(actual_elm, true, true);
        return actual_str;
    }
    
    /**
     * Logs the EXPLAIN of a logical plan.
     * 
     * @param plan
     *            the plan to explain
     */
    protected void logLogicalPlanExplain(LogicalPlan plan)
    {
        log.info("\n" + plan.toExplainString());
    }
    
    /**
     * Logs the XML serialization of the logical plan.
     * 
     * @param plan
     *            the plan to serialize
     */
    protected void logLogicalPlanXml(LogicalPlan plan)
    {
        log.info("\n" + XmlUtil.serializeDomToString(new PlanXmlSerializer().serializeLogicalPlan(plan), true, true));
    }
    
    /**
     * Logs the XML serialization of the physical plan.
     * 
     * @param plan
     *            the plan to serialize
     */
    protected void logPhysicalPlanXml(PhysicalPlan plan)
    {
        log.info("\n" + XmlUtil.serializeDomToString(new PlanXmlSerializer().serializePhysicalPlan(plan), true, true));
    }
    
    /**
     * Logs an output value.
     * 
     * @param value
     *            the value to log
     * @throws QueryExecutionException
     *             if something goes wrong
     */
    protected void logOutputValueXml(Value value) throws QueryExecutionException
    {
        Document d = XmlUtil.createDocument();
        ValueXmlSerializer.serializeValue(value, "root", d);
        log.info("\n" + XmlUtil.serializeDomToString(d, true, true));
    }
    
    /**
     * Logs an output value.
     * 
     * @param value
     *            the value to log
     * @throws QueryExecutionException
     *             if something goes wrong
     */
    protected void logOutputValueExplain(Value value) throws QueryExecutionException
    {
        log.info("\n" + value.toString());
    }
    
    /**
     * Logs the type of an output value.
     * 
     * @param value
     *            the type to log
     * @throws QueryExecutionException
     *             if something goes wrong
     */
    protected void logOutputType(Type type) throws QueryExecutionException
    {
        Document d = XmlUtil.createDocument();
        TypeXmlSerializer.serializeType(type, "root", d);
        log.info("\n" + XmlUtil.serializeDomToString(d, true, true));
    }
}
