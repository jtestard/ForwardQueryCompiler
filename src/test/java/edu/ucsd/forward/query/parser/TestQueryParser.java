/**
 * 
 */
package edu.ucsd.forward.query.parser;

import java.util.List;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryParsingException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.ast.QueryConstruct;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * Tests the query parser.
 * 
 * @author Michalis Petropoulos
 * 
 */
@Test
public class TestQueryParser extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestQueryParser.class);
    
    /**
     * Tests value expression precedence.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testValueExpressionPrecedence() throws Exception
    {
        verifyAst("TestQueryParser-testValueExpressionPrecedence");
    }
    
    /**
     * Tests joins.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testJoins() throws Exception
    {
        verifyAst("TestQueryParser-testJoins");
    }
    
    /**
     * Tests WITH clause.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testWithClause() throws Exception
    {
        verifyAst("TestQueryParser-testWithClause");
    }
    
    /**
     * Tests tuple all item.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testTupleAllItem() throws Exception
    {
        verifyAst("TestQueryParser-testTupleAllItem");
    }
    
    /**
     * Tests that token manager errors are caught.
     */
    @Test(groups = FAILURE)
    // Romain: The SELECT clause is now parsed differently and cannot generate the given error message. Not sure how to generate
    // this error message or if it's even relevant anymore.
    public void testTokenManagerErrors()
    {
        try
        {
            verifyAst("TestQueryParser-testTokenMgrError");
        }
        catch (Exception e)
        {
            assert (e instanceof QueryParsingException);
            log.error("\n" + ((QueryParsingException) e).getSingleLineMessageWithLocation());
            assert (((QueryParsingException) e).getSingleLineMessageWithLocation().equals("Lexical error at line 3, column 20.  Encountered: \"<\" (123), after : \">\" TestQueryParser-testTokenMgrError.xml "));
        }
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
        
        String actual_str = getQueryExpression(0);
        
        List<AstTree> ast_trees = QueryProcessorFactory.getInstance().parseQuery(actual_str, location);
        assert (ast_trees.size() == 1);
        QueryConstruct actual_expr = ast_trees.get(0).getRoot();
        StringBuilder actual = new StringBuilder();
        actual_expr.toQueryString(actual, 0, null);
        
        String expected_str = getQueryExpression(1);
        StringBuilder exp = new StringBuilder();
        QueryProcessorFactory.getInstance().parseQuery(expected_str, location).get(0).toQueryString(exp, 0, null);
        expected_str = exp.toString();
        
        assertEquals(expected_str, actual.toString());
    }
    
}
