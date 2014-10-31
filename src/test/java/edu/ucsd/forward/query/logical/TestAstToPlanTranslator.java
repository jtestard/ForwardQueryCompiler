/**
 * 
 */
package edu.ucsd.forward.query.logical;

import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.source.jdbc.JdbcDataSource;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.visitors.ConsistencyChecker;
import edu.ucsd.forward.query.logical.visitors.DistributedNormalFormChecker;
import edu.ucsd.forward.query.logical.visitors.InitialNormalFormChecker;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * Tests the translation of an AST tree in semantic normal form to a logical plan.
 * 
 * @author Michalis Petropoulos
 * @author Romain Vernoux
 */
@Test
public class TestAstToPlanTranslator extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger  log   = Logger.getLogger(TestAstToPlanTranslator.class);
    
    /**
     * If this flag is enabled, the testcases log information useful for creating/debugging test cases.
     */
    private static final boolean DEBUG = false;
    
    /**
     * Tests parsing a value expression.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testValueExpression() throws Exception
    {
        verify("TestAstToPlanTranslator-testValueExpression");
    }
    
    /**
     * Tests string value expressions.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testStringValueExpression() throws Exception
    {
        verify("TestAstToPlanTranslator-testStringValueExpression");
    }
    
    /**
     * Tests numeric value expressions.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testNumericValueExpression() throws Exception
    {
        verify("TestAstToPlanTranslator-testNumericValueExpression");
    }
    
    /**
     * Tests boolean value expressions.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testBooleanValueExpression() throws Exception
    {
        verify("TestAstToPlanTranslator-testBooleanValueExpression");
        verify("TestAstToPlanTranslator-testBooleanValueExpressionExists");
        verify("TestAstToPlanTranslator-testBooleanValueExpressionExistsGroupBy");
    }
    
    /**
     * Tests case specifications.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testCaseSpecification() throws Exception
    {
        verify("TestAstToPlanTranslator-testCaseSpecification");
    }
    
    /**
     * Tests the select clause of a query specification.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testSelectClause() throws Exception
    {
        verify("TestAstToPlanTranslator-testSelectClauseAll");
        verify("TestAstToPlanTranslator-testSelectClauseDistinct");
    }
    
    /**
     * Tests the select clause of a query specification containing a subquery.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testSelectClauseSubQuery() throws Exception
    {
        verify("TestAstToPlanTranslator-testSelectClauseSubQuery");
        verify("TestAstToPlanTranslator-testSelectClauseSubQueryGroupBy");
    }
    
    /**
     * Tests the select clause of a query specification containing a tuple constructor.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testSelectClauseTuple() throws Exception
    {
        verify("TestAstToPlanTranslator-testSelectClauseTuple");
        verify("TestAstToPlanTranslator-testSelectClauseTupleSubQuery");
        verify("TestAstToPlanTranslator-testSelectClauseTupleGroupBy");
    }
    
    /**
     * Tests the select clause of a query specification containing a switch value constructor.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testSelectClauseSwitch() throws Exception
    {
        verify("TestAstToPlanTranslator-testSelectClauseSwitch");
    }
    
    /**
     * Tests the from clause of a query specification.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testFromClause() throws Exception
    {
        verify("TestAstToPlanTranslator-testFromClause");
        verify("TestAstToPlanTranslator-testFromClause-SelfProduct");
    }
    
    /**
     * Tests the from clause of a query specification.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testFromClauseUnnest() throws Exception
    {
        verify("TestAstToPlanTranslator-testFromClauseUnnestInner");
        verify("TestAstToPlanTranslator-testFromClauseUnnestInnerSubquery");
        verify("TestAstToPlanTranslator-testFromClauseUnnestOuter");
        verify("TestAstToPlanTranslator-testFromClauseUnnestOuterSubquery");
    }
    
    /**
     * Tests the where clause of a query specification.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testWhereClause() throws Exception
    {
        verify("TestAstToPlanTranslator-testWhereClause");
        verify("TestAstToPlanTranslator-testWhereClauseNoFromClause");
    }
    
    /**
     * Tests the group by clause of a query specification.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testGroupByClause() throws Exception
    {
        verify("TestAstToPlanTranslator-testGroupByClause");
    }
    
    /**
     * Tests the having clause of a query specification.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testHavingClause() throws Exception
    {
        verify("TestAstToPlanTranslator-testHavingClause");
        verify("TestAstToPlanTranslator-testHavingClauseNoAggs");
        verify("TestAstToPlanTranslator-testHavingClauseOnlyAggs");
    }
    
    /**
     * Tests set operation expressions.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testSetOpExpression() throws Exception
    {
        verify("TestAstToPlanTranslator-testSetOpExpression");
        verify("TestAstToPlanTranslator-testSetOpExpressionOrderByOutput");
    }
    
    /**
     * Tests query expressions.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testQueryExpression() throws Exception
    {
        verify("TestAstToPlanTranslator-testQueryExpression");
        verify("TestAstToPlanTranslator-testQueryExpressionLimit");
        verify("TestAstToPlanTranslator-testQueryExpressionOrderByOutput");
        verify("TestAstToPlanTranslator-testQueryExpressionOrderByOutputDistinct");
    }
    
    /**
     * Tests navigation.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testNavigation() throws Exception
    {
        verify("TestAstToPlanTranslator-testNavigation");
    }
    
    /**
     * Tests schema object definition.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testCreateDataObject() throws Exception
    {
        verify("TestAstToPlanTranslator-testCreateDataObjectCollection");
        verify("TestAstToPlanTranslator-testCreateDataObjectScalar");
        verify("TestAstToPlanTranslator-testCreateDataObjectDefaultValue");
        verify("TestAstToPlanTranslator-testCreateDataObjectDefaultValue2");
    }
    
    /**
     * Tests schema object drop.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testDropDataObject() throws Exception
    {
        verify("TestAstToPlanTranslator-testDropDataObject");
    }
    
    /**
     * Tests insert statement.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testInsert() throws Exception
    {
        verify("TestAstToPlanTranslator-testInsertFlatSubQuery");
        verify("TestAstToPlanTranslator-testInsertFlatTuple");
        verify("TestAstToPlanTranslator-testInsertFlatValues");
        // verify("TestAstToPlanTranslator-testInsertFlatValuesDefaultNull"); // Feature not implemented
        verify("TestAstToPlanTranslator-testInsertFlatDefaultValues");
        verify("TestAstToPlanTranslator-testInsertNestedSubQuery");
    }
    
    /**
     * Tests delete statement.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testDelete() throws Exception
    {
        verify("TestAstToPlanTranslator-testDeleteFlat");
        verify("TestAstToPlanTranslator-testDeleteFlatSubQuery");
        verify("TestAstToPlanTranslator-testDeleteNested");
    }
    
    /**
     * Tests update statement.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testUpdate() throws Exception
    {
        verify("TestAstToPlanTranslator-testUpdateRootScalar");
        verify("TestAstToPlanTranslator-testUpdateRootTuple");
        verify("TestAstToPlanTranslator-testUpdateRootTupleAttribute");
        verify("TestAstToPlanTranslator-testUpdateRootCollection");
        verify("TestAstToPlanTranslator-testUpdateCollection");
        verify("TestAstToPlanTranslator-testUpdateFlat");
        verify("TestAstToPlanTranslator-testUpdateFlatSubQuery");
        verify("TestAstToPlanTranslator-testUpdateNested");
    }
    
    /**
     * Tests WITH clause.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testWithClause() throws Exception
    {
        verify("TestAstToPlanTranslator-testWithClause");
        verify("TestAstToPlanTranslator-testWithClauseNested");
        verify("TestAstToPlanTranslator-testNestedWithClause");
    }
    
    /**
     * Tests external functions.
     * 
     * @throws Exception
     *             if something goes wrong.
     * 
     *             Needs PostgreSQL 8.4 or above
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testExternalFunction() throws Exception
    {
        JdbcDataSource data_source = null;
        try
        {
            data_source = setupJdbcDataSource(this.getClass(), "TestAstToPlanTranslator-testExternalFunctionSetup",
                                              "TestAstToPlanTranslator-testExternalFunctionCleanup");
            
            // verify("TestAstToPlanTranslator-testExternalFunctionVoid");
            // verify("TestAstToPlanTranslator-testExternalFunctionVoidSelect");
            verify("TestAstToPlanTranslator-testExternalFunctionScalar");
            verify("TestAstToPlanTranslator-testExternalFunctionScalarComposition");
            verify("TestAstToPlanTranslator-testExternalFunctionScalarSelect");
            // verify("TestAstToPlanTranslator-testExternalFunctionScalarFrom");
            verify("TestAstToPlanTranslator-testExternalFunctionTuple");
            verify("TestAstToPlanTranslator-testExternalFunctionTupleSelect");
            // verify("TestAstToPlanTranslator-testExternalFunctionTupleFrom");
            verify("TestAstToPlanTranslator-testExternalFunctionCollection");
            verify("TestAstToPlanTranslator-testExternalFunctionCollectionSelect");
            verify("TestAstToPlanTranslator-testExternalFunctionCollectionFrom");
        }
        catch (Exception e)
        {
            throw e;
        }
        finally
        {
            cleanupJdbcDataSource(this.getClass(), data_source, "TestAstToPlanTranslator-testExternalFunctionCleanup");
        }
    }
    
    /**
     * Tests FROM clauses with BAG literals.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testFromBags() throws Exception
    {
        verify("TestAstToPlanTranslator-testFromBag");
        verify("TestAstToPlanTranslator-testFromBagCast");
        verify("TestAstToPlanTranslator-testFromBagCast2");
    }
    
    /**
     * Tests bags of non-tuples.
     * 
     * @throws Exception
     *             if something goes wrong
     */
    public void testBagOfValues() throws Exception
    {
        verify("TestAstToPlanTranslator-testBagOfValuesInput");
        verify("TestAstToPlanTranslator-testBagOfValuesInput2");
        verify("TestAstToPlanTranslator-testBagOfValuesInputOutput");
        verify("TestAstToPlanTranslator-testBagOfValuesInputOutput2");
        verify("TestAstToPlanTranslator-testBagOfValuesInputOutput3");
        verify("TestAstToPlanTranslator-testBagOfValuesInputOutputDistinct");
        verify("TestAstToPlanTranslator-testBagOfValuesInputAggregate");
        verify("TestAstToPlanTranslator-testBagOfValuesInputOutputAggregate");
        verify("TestAstToPlanTranslator-testBagOfValuesOutput");
        verify("TestAstToPlanTranslator-testBagOfValuesInputJoin");
        verify("TestAstToPlanTranslator-testBagOfValuesInputOutputGroupBySelect");
    }
    
    /**
     * Tests lists (ordered collections) of tuples.
     * 
     * @throws Exception
     *             if something goes wrong
     */
    public void testListsOfTuples() throws Exception
    {
        verify("TestAstToPlanTranslator-testListsInput");
        verify("TestAstToPlanTranslator-testListsInputOutput2");
        verify("TestAstToPlanTranslator-testListsOutput");
        verify("TestAstToPlanTranslator-testListsInputOutput");
        verify("TestAstToPlanTranslator-testListsInputOutputJoin");
        verify("TestAstToPlanTranslator-testListsInputOutputJoin2");
        verify("TestAstToPlanTranslator-testListsInputOutputNested");
        verify("TestAstToPlanTranslator-testListsInputOutputNested2");
        verify("TestAstToPlanTranslator-testListsInputOutputGroupBy");
    }
    
    /**
     * Tests lists (ordered collections) of non-tuples.
     * 
     * @throws Exception
     *             if something goes wrong
     */
    public void testListOfValues() throws Exception
    {
        verify("TestAstToPlanTranslator-testListsValuesInput");
        verify("TestAstToPlanTranslator-testListsValuesInput2");
        verify("TestAstToPlanTranslator-testListsValuesInput3");
        verify("TestAstToPlanTranslator-testListsValuesInput4");
    }
    
    /**
     * Tests that the parser handles correctly escaped column names.
     * 
     * @throws Exception
     *             if something goes wrong
     */
    public void testQuotedColumnName() throws Exception
    {
        verify("TestAstToPlanTranslator-testQuotedColumnName");
        verify("TestAstToPlanTranslator-testQuotedColumnName2");
    }
    
    /**
     * Tests CASE/WHEN short circuiting.
     * 
     * @throws Exception
     *             if something goes wrong
     */
    public void testCaseWhenShortCircuiting() throws Exception
    {
        verify("TestAstToPlanTranslator-testCaseWhenShortCircuiting");
        verify("TestAstToPlanTranslator-testCaseWhenShortCircuitingNull");
    }
    
    /**
     * Tests AND/OR short circuiting.
     * 
     * @throws Exception
     *             if something goes wrong
     */
    public void testLogicalCompShortCircuiting() throws Exception
    {
        verify("TestAstToPlanTranslator-testLogicalCompShortCircuiting");
    }
    
    /**
     * Verifies the output of the query processor.
     * 
     * @param relative_file_name
     *            the relative path of the XML file specifying the test case.
     * @throws Exception
     *             if an error occurs.
     */
    protected void verify(String relative_file_name) throws Exception
    {
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), relative_file_name + ".xml");
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        String query_str_before = null;
        
        // Parse the query
        String query_expr = getQueryExpression(0);
        List<AstTree> ast_trees = qp.parseQuery(query_expr, new LocationImpl(relative_file_name + ".xml"));
        assert (ast_trees.size() == 1);
        
        // Save the AST
        StringBuilder sb = new StringBuilder();
        ast_trees.get(0).toQueryString(sb, 0, null);
        query_str_before = sb.toString();
        
        // Translate the AST
        LogicalPlan actual = qp.translate(Collections.singletonList(ast_trees.get(0)), uas).get(0);
        if (DEBUG) logLogicalPlanExplain(actual);
        if (DEBUG) logLogicalPlanXml(actual);
        ConsistencyChecker.getInstance(uas).check(actual);
        
        // Check against normal form
        new InitialNormalFormChecker(uas).check(actual);
        
        // Compare expected/actual output logical plan
        LogicalPlan expected = getLogicalPlan(0);
        checkLogicalPlan(actual, expected, uas);
        
        // Make sure the input AST is not modified.
        sb = new StringBuilder();
        ast_trees.get(0).toQueryString(sb, 0, null);
        String query_str_after = sb.toString();
        assertEquals(query_str_before, query_str_after);
        
        // Make sure the input AST can be copied.
        sb = new StringBuilder();
        ast_trees.get(0).copy().toQueryString(sb, 0, null);
        String query_str_copy = sb.toString();
        assertEquals(query_str_before, query_str_copy);
        
        // Proceed with the compilation
        actual = qp.distribute(Collections.singletonList(actual), uas).get(0);
        ConsistencyChecker.getInstance(uas).check(actual);
        DistributedNormalFormChecker.getInstance(uas).check(actual);
        PhysicalPlan actual_physical = qp.generate(Collections.singletonList(actual), uas).get(0);
        
        // Check the output value
        Value output_value = qp.createEagerQueryResult(actual_physical, uas).getValue();
        if (DEBUG) logOutputValueExplain(output_value);
        if (DEBUG) logOutputValueXml(output_value);
        if (DEBUG) logOutputType(actual_physical.getLogicalPlan().getOutputType());
        checkOutputValue(output_value, uas);
        
    }
}
