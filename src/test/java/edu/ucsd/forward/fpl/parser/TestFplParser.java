/**
 * 
 */
package edu.ucsd.forward.fpl.parser;

import java.util.List;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.fpl.AbstractFplTestCase;
import edu.ucsd.forward.fpl.FplInterpreterFactory;
import edu.ucsd.forward.fpl.ast.Definition;

/**
 * Tests the action parser.
 * 
 * @author Yupeng
 * 
 */
@Test
public class TestFplParser extends AbstractFplTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestFplParser.class);
    
    /**
     * Tests function creation.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testCreateFunction() throws Exception
    {
        verifyAst("TestFplParser-testCreateFunction");
    }
    
    /**
     * Tests action creation.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testCreateAction() throws Exception
    {
        verifyAst("TestFplParser-testCreateActionWithFunction");
        verifyAst("TestFplParser-testCreateAction");        
        verifyAst("TestFplParser-testCreateActionReturns");
        verifyAst("TestFplParser-testCreateActionQuotedPath");
        verifyAst("TestFplParser-testCreateActionEmptyPath");
    }
    
    /**
     * Tests parameter declarations.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testParameterDeclaration() throws Exception
    {
        verifyAst("TestFplParser-testParameterDeclaration");
        // We do not support the function polymorphism, therefore not support parameter's default value.
        // verifyAst("TestFplParser-testParameterDeclarationDefault");
    }
    
    /**
     * Tests declare section.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testDeclareSection() throws Exception
    {
        verifyAst("TestFplParser-testDeclareSection");
        verifyAst("TestFplParser-testDeclareSectionDefault");
    }
    
    /**
     * Tests assignment statement.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testAssignmentStatement() throws Exception
    {
        verifyAst("TestFplParser-testAssignmentStatement");
    }
    
    /**
     * Tests if statement.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testIfStatement() throws Exception
    {
        verifyAst("TestFplParser-testIfStatement");
    }
    
    /**
     * Tests return statement.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testReturnStatement() throws Exception
    {
        verifyAst("TestFplParser-testReturnStatement");
    }
    
    /**
     * Tests exception handler.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testExceptionHandler() throws Exception
    {
        verifyAst("TestFplParser-testExceptionHandler");
    }
    
    /**
     * Tests raise statement.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testRaiseStatement() throws Exception
    {
        verifyAst("TestFplParser-testRaiseStatement");
    }
    
    /**
     * Tests an example action as in the specification.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testSendInvitation() throws Exception
    {
        verifyAst("TestFplParser-testSendInvitation");
    }
    
    /**
     * Verifies an AST tree.
     * 
     * @param relative_file_name
     *            the filename of the query string.
     * @throws Exception
     *             if an error occurs.
     */
    protected void verifyAst(String relative_file_name) throws Exception
    {
        Location location = new LocationImpl(relative_file_name + ".xml");
        
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), relative_file_name + ".xml");
        
        String action_str = getActionExpression(0);
        
        List<Definition> ast_trees = FplInterpreterFactory.getInstance().parseFplCode(action_str, location);
        StringBuilder sb = new StringBuilder();
        for (Definition definition : ast_trees)
        {
            definition.toActionString(sb, 0);
        }
        String actual_str = sb.toString();
        
        String expected_expr = getActionExpression(1);
        List<Definition> expected_ast_trees = FplInterpreterFactory.getInstance().parseFplCode(expected_expr, location);
        sb = new StringBuilder();
        for (Definition definition : expected_ast_trees)
        {
            definition.toActionString(sb, 0);
        }
        String expected_str = sb.toString();
        assertEquals(expected_str, actual_str);
    }
}
