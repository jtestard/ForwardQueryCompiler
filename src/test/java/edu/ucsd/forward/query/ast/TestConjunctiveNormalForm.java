/**
 * 
 */
package edu.ucsd.forward.query.ast;

import java.util.List;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.visitors.ConjunctiveNormalFormConverter;

/**
 * Tests the CNF converter.
 * 
 * @author Michalis Petropoulos
 * 
 */
@Test
public class TestConjunctiveNormalForm extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestConjunctiveNormalForm.class);
    
    /**
     * Tests pushing negations in a boolean value expression.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testNegationsIn() throws Exception
    {
        verifyAst("TestConjunctiveNormalForm-testNegationsIn");
    }
    
    /**
     * Tests distributing ORs over ANDs in a boolean value expression.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testDistributeOrOverAnd() throws Exception
    {
        verifyAst("TestConjunctiveNormalForm-testDistributeOrOverAnd");
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
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), relative_file_name + ".xml");
        
        String actual_str = getQueryExpression(0);
        
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        
        List<AstTree> ast_trees = qp.parseQuery(actual_str, new LocationImpl(relative_file_name + ".xml"));
        assert (ast_trees.size() == 1);
        // Get the expression in the first select item.
        QuerySpecification query_spec = (QuerySpecification) ast_trees.get(0).getRoot();
        ValueExpression actual_expr = (ValueExpression) query_spec.getSelectItems().get(0).getChildren().get(0);
        // Convert to CNF
        ValueExpression actual_cnf = (ValueExpression) ConjunctiveNormalFormConverter.convert(actual_expr);
        StringBuilder actual = new StringBuilder();
        actual_cnf.toQueryString(actual,0, null);
        
        String expected_str = getQueryExpression(1);
        ast_trees = qp.parseQuery(expected_str, new LocationImpl(relative_file_name + ".xml"));
        assert (ast_trees.size() == 1);
        // Get the expression in the first select item.
        query_spec = (QuerySpecification) ast_trees.get(0).getRoot();
        ValueExpression expected_expr = (ValueExpression) query_spec.getSelectItems().get(0).getChildren().get(0);
        StringBuilder expected = new StringBuilder();
        expected_expr.toQueryString(expected, 0, null);
        
        assertEquals(expected.toString(), actual.toString());
    }
    
}
