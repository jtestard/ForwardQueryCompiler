/**
 * 
 */
package edu.ucsd.forward.fpl.plan;

import static edu.ucsd.forward.data.xml.ValueXmlSerializer.serializeDataTree;
import static edu.ucsd.forward.xml.XmlUtil.createDocument;
import static edu.ucsd.forward.xml.XmlUtil.getNonEmptyAttribute;
import static edu.ucsd.forward.xml.XmlUtil.serializeDomToString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.fpl.AbstractFplTestCase;
import edu.ucsd.forward.fpl.FplInterpreter;
import edu.ucsd.forward.fpl.FplInterpreterFactory;
import edu.ucsd.forward.fpl.ast.ActionDefinition;
import edu.ucsd.forward.fpl.ast.Definition;
import edu.ucsd.forward.fpl.ast.FunctionDefinition;
import edu.ucsd.forward.fpl.plan.xml.FplXmlParser;
import edu.ucsd.forward.fpl.plan.xml.FplXmlSerializer;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.function.FunctionRegistry;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.warning.Warning;
import edu.ucsd.forward.warning.WarningCollectorFactory;

/**
 * Tests execution of FPL plans.
 * 
 * @author Yupeng
 * 
 */
@Test
public class TestFplPlans extends AbstractFplTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log          = Logger.getLogger(TestFplPlans.class);
    
    private static final String EXPECTED_ELM = "expected";
    
    private static final String ACTUAL_ELM   = "actual";
    
    /**
     * Tests a basic function that consists only of DML statements without any control flow.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testBasic() throws Exception
    {
        verify("TestFplPlan-testBasic", true);
        // FIXME Move to TestDistributedNormalForm
        // Tests that FPL functions are not pushed to JDBC data sources
        verify("TestFplPlan-testBasicJdbc", false);
    }
    
    /**
     * Tests the exception raising and catching.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testException() throws Exception
    {
        verify("TestFplPlan-testException", true);
        verify("TestFplPlan-testExceptionOthers", true);
    }
    
    /**
     * Tests assigning variable.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testVariableAssignment() throws Exception
    {
        verify("TestFplPlan-testVariableAssignment", true);
    }
    
    /**
     * Tests the variable with default value.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testDefaultVariable() throws Exception
    {
        verify("TestFplPlan-testDefaultVariable", true);
    }
    
    /**
     * Tests if statements.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testIfStatement() throws Exception
    {
        verify("TestFplPlan-testIfStatementNested", true);
        verify("TestFplPlan-testIfStatement", true);
        verify("TestFplPlan-testIfStatementWithouElse", true);
    }
    
    /**
     * Tests the function call.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testFunctoinCall() throws Exception
    {
        verify("TestFplPlan-testFunctionCallInFunction", true);
        verify("TestFplPlan-testFunctionCallInQuery", true);
        verify("TestFplPlan-testFunctionCallWithCastingParameter", true);
        verify("TestFplPlan-testFunctionCallSameArgumetns", true);
        verify("TestFplPlan-testFunctionCallInQuery2", true);
    }
    
    /**
     * Tests the omitted return statement.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testOmittedReturnStatement() throws Exception
    {
        verify("TestFplPlan-testOmittedReturnStatement", true);
    }
    
    /**
     * Tests the recursive invocation of fpl function.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testRecursion() throws Exception
    {
        verify("TestFplPlan-testRecursion", true);
    }
    
    /**
     * Tests the warning generation for typo in assignment statement.
     * 
     * @throws Exception
     *             if anything wrong happens
     */
    public void testAssignmentTypoWarning() throws Exception
    {
        verify("TestFplPlan-testAssignmentTypoWarning", false);
        Warning warning = WarningCollectorFactory.getInstance().getWarnings().get(0);
        assertEquals("Typo? Assignment statement uses \":=\" but not \"=\"", warning.getMessage());
    }
    
    /**
     * Referencing update table within subquery.
     * 
     * @throws Exception
     *             if anything wrong happens
     */
    public void testUpdateCollectionWithSubquery() throws Exception
    {
        verify("TestFplPlan-testUpdateCollectionWithSubquery", true);
    }
    
    /**
     * Verifies a function execution..
     * 
     * @param relative_file_name
     *            the filename of the test case configuration.
     * 
     * @param test_serialization
     *            whether test the fpl function serialization.
     * @throws Exception
     *             if an error occurs.
     */
    private void verify(String relative_file_name, boolean test_serialization) throws Exception
    {
        Location location = new LocationImpl(relative_file_name + ".xml");
        
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), relative_file_name + ".xml");
        
        String action_str = getActionExpression(0);
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        // Parse and compile actions
        FplInterpreter interpreter = FplInterpreterFactory.getInstance();
        interpreter.setUnifiedApplicationState(uas);
        List<Definition> definitions = interpreter.parseFplCode(action_str, location);
        for (Definition definition : definitions)
        {
            if (definition instanceof ActionDefinition) continue;
            interpreter.registerFplFunction((FunctionDefinition) definition);
        }
        for (Definition definition : definitions)
        {
            if (definition instanceof ActionDefinition) continue;
            interpreter.compileFplFunction((FunctionDefinition) definition);
        }
        
        evaluateFpl(location);
        
        if (test_serialization)
        {
            List<Element> func_elms = new ArrayList<Element>();
            Map<String, FplFunction> name_funcs = new LinkedHashMap<String, FplFunction>();
            // Check fpl function can serialize
            for (Definition definition : definitions)
            {
                if (definition instanceof ActionDefinition) continue;
                FplFunction fpl_func = (FplFunction) FunctionRegistry.getInstance().getFunction(((FunctionDefinition) definition).getName());
                FplXmlSerializer serializer = new FplXmlSerializer();
                Element fpl_func_elm = serializer.serializeFplFunction(fpl_func);
                func_elms.add(fpl_func_elm);
                // log.info(serializeDomToString(fpl_func_elm, true, true));
                
                // remove the existing function
                FunctionRegistry.getInstance().removeApplicationFunction(fpl_func.getName());
                FplFunction function_plan = FplXmlParser.parseFplFunctionDefinition(getUnifiedApplicationState(), fpl_func_elm);
                name_funcs.put(fpl_func.getName(), function_plan);
                FunctionRegistry.getInstance().addApplicationFunction(function_plan);
                // FunctionRegistry.getInstance().addApplicationFunction(fpl_func);
            }
            
            // Parse the fpl instructions
            for (Element func_elm : func_elms)
            {
                String func_name = getNonEmptyAttribute(func_elm, FplXmlSerializer.NAME_ATTR);
                FplXmlParser.parseFplFunctionImpl(uas, func_elm, name_funcs.get(func_name));
            }
            
            // Parse the test case from the XML file again
            parseTestCase(this.getClass(), relative_file_name + ".xml");
            evaluateFpl(location);
        }
    }
    
    private void evaluateFpl(Location location) throws Exception
    {
        UnifiedApplicationState uas = getUnifiedApplicationState();
        // Gets the query expression, which is the entry point
        String query_str = getQueryExpression(0);
        QueryProcessor processor = QueryProcessorFactory.getInstance();
        AstTree ast_tree = processor.parseQuery(query_str, location).get(0);
        PhysicalPlan physical_plan = processor.compile(Collections.singletonList(ast_tree), uas).get(0);
        processor.createEagerQueryResult(physical_plan, uas).getValue();
        
        // Check if the query execution result is correct
        DataTree expected_output = null;
        try
        {
            expected_output = uas.getDataSource(EXPECTED_ELM).getDataObject(EXPECTED_ELM);
        }
        catch (DataSourceException e)
        {
            // This should never happen
            assert (false);
        }
        DataTree actual_output = null;
        try
        {
            actual_output = uas.getDataSource(ACTUAL_ELM).getDataObject(ACTUAL_ELM);
        }
        catch (DataSourceException e)
        {
            // This should never happen
            assert (false);
        }
        
        Document actual_doc = createDocument();
        serializeDataTree(actual_output, actual_doc);
        
        Document expected_doc = createDocument();
        serializeDataTree(expected_output, expected_doc);
        
        // Check that the XML trees are isomorphic
        assertEquals(serializeDomToString(expected_doc, true, true), serializeDomToString(actual_doc, true, true));
    }
}
