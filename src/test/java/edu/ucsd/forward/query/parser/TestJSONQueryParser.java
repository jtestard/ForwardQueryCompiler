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

/**
 * Tests the equivalence between SQL and JSON query parser.
 * 
 * @author Romain Vernoux
 * 
 */
@Test
public class TestJSONQueryParser extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestJSONQueryParser.class);
    
    
    
    /**
     * Test aliases without the AS keyword.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testSimpleAlias() throws Exception
    {
        verifyAst("TestJSONQueryParser-testSimpleAlias");
    }
    
    /**
     * Test aliases with the AS keyword.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testFullAlias() throws Exception
    {
        verifyAst("TestJSONQueryParser-testFullAlias");
    }
    
    /**
     * Test tuple literals
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testTuple() throws Exception
    {
        verifyAst("TestJSONQueryParser-testTupleValue");
    }
    
    /**
     * Test tuple literals with mixed syntax
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testMixedTuple() throws Exception
    {
        verifyAst("TestJSONQueryParser-testMixedTupleValue");
    }
    
    /**
     * Test bag literals
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testBag() throws Exception
    {
        verifyAst("TestJSONQueryParser-testBag");
    }
    
    /**
     * Test bag literals in the FROM clause
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testFromBag() throws Exception
    {
        verifyAst("TestJSONQueryParser-testFromBag");
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
        
        String sql_str = getQueryExpression(0);
        String json_str = getQueryExpression(1);
        
        List<AstTree> sql_ast_trees = QueryProcessorFactory.getInstance().parseQuery(sql_str, location);
        assert (sql_ast_trees.size() == 1);
        QueryConstruct sql_expr = (QueryConstruct) sql_ast_trees.get(0).getRoot();
        StringBuilder sql = new StringBuilder();
        sql_expr.toQueryString(sql, 0, null);
        
        List<AstTree> json_ast_trees = QueryProcessorFactory.getInstance().parseQuery(json_str, location);
        assert (json_ast_trees.size() == 1);
        QueryConstruct json_expr = (QueryConstruct) json_ast_trees.get(0).getRoot();
        StringBuilder json = new StringBuilder();
        json_expr.toQueryString(json, 0, null);
        
        log.debug(sql.toString());
        assertEquals(sql.toString(), json.toString());
    }
    
}
