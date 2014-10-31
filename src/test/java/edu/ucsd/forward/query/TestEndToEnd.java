/**
 * 
 */
package edu.ucsd.forward.query;

import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.ExceptionMessages.QueryCompilation;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.logical.TestAstToPlanTranslator;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.visitors.ConsistencyChecker;
import edu.ucsd.forward.query.logical.visitors.DistributedNormalFormChecker;
import edu.ucsd.forward.query.logical.visitors.InitialNormalFormChecker;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.source_wrapper.sql.TestPlanToAstTranslator;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * Tests the query processor end to end.
 * 
 * @author Michalis Petropoulos
 * 
 */
@Test
public class TestEndToEnd extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger  log   = Logger.getLogger(TestEndToEnd.class);
    
    /**
     * If this flag is enabled, the testcases log information useful for creating/debugging test cases.
     */
    private static final boolean DEBUG = false;
    
    // //////////////////////// LogicalPlanBuilder test cases ////////////////////////////
    
    /**
     * Tests the LogicalPlanBuilder test cases end-to-end.
     * 
     * @throws Exception
     *             if something goes wrong
     */
    public void testLogicalPlanBuilderTestCases() throws Exception
    {
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testValueExpression", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testStringValueExpression", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testNumericValueExpression", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testBooleanValueExpression", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testBooleanValueExpressionExists", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testBooleanValueExpressionExistsGroupBy", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testCaseSpecification", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testSelectClauseAll", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testSelectClauseDistinct", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testSelectClauseSubQuery", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testSelectClauseSubQueryGroupBy", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testSelectClauseTuple", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testSelectClauseTupleSubQuery", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testSelectClauseTupleGroupBy", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testFromClause", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testFromClause-SelfProduct", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testFromClauseUnnestInner", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testFromClauseUnnestInnerSubquery", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testFromClauseUnnestOuter", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testFromClauseUnnestOuterSubquery", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testWhereClause", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testWhereClauseNoFromClause", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testGroupByClause", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testHavingClause", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testHavingClauseNoAggs", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testHavingClauseOnlyAggs", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testSetOpExpression", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testSetOpExpressionOrderByOutput", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testQueryExpression", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testQueryExpressionLimit", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testQueryExpressionOrderByOutput", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testQueryExpressionOrderByOutputDistinct", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testNavigation", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testCreateDataObjectCollection", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testCreateDataObjectScalar", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testCreateDataObjectDefaultValue", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testCreateDataObjectDefaultValue2", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testDropDataObject", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testInsertFlatSubQuery", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testInsertFlatTuple", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testInsertFlatValues", true);
        // verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testInsertFlatValuesDefaultNull", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testInsertFlatDefaultValues", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testInsertNestedSubQuery", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testDeleteFlat", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testDeleteFlatSubQuery", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testDeleteNested", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testUpdateRootScalar", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testUpdateRootTuple", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testUpdateRootTupleAttribute", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testUpdateRootCollection", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testUpdateCollection", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testUpdateFlat", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testUpdateFlatSubQuery", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testUpdateNested", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testWithClause", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testWithClauseNested", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testNestedWithClause", true);
        // verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testExternalFunctionVoid", true);
        // verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testExternalFunctionVoidSelect", true);
        // verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testExternalFunctionScalar", true);
        // verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testExternalFunctionScalarComposition", true);
        // verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testExternalFunctionScalarSelect", true);
        // verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testExternalFunctionScalarFrom", true);
        // verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testExternalFunctionTuple", true);
        // verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testExternalFunctionTupleSelect", true);
        // verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testExternalFunctionTupleFrom", true);
        // verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testExternalFunctionCollection", true);
        // verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testExternalFunctionCollectionSelect", true);
        // verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testExternalFunctionCollectionFrom", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testFromBag", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testFromBagCast", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testFromBagCast2", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testBagOfValuesInput", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testBagOfValuesInput2", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testBagOfValuesInputOutput", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testBagOfValuesInputOutput2", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testBagOfValuesInputOutput3", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testBagOfValuesInputOutputDistinct", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testBagOfValuesInputAggregate", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testBagOfValuesInputOutputAggregate", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testBagOfValuesOutput", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testBagOfValuesInputJoin", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testBagOfValuesInputOutputGroupBySelect", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testListsInput", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testListsInputOutput2", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testListsOutput", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testListsInputOutput", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testListsInputOutputJoin", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testListsInputOutputJoin2", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testListsInputOutputNested", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testListsInputOutputNested2", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testListsInputOutputGroupBy", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testListsValuesInput", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testListsValuesInput2", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testListsValuesInput3", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testListsValuesInput4", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testQuotedColumnName", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testQuotedColumnName2", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testCaseWhenShortCircuiting", true);
        verify(TestAstToPlanTranslator.class, "TestAstToPlanTranslator-testLogicalCompShortCircuiting", true);
        
    }
    
    // //////////////////////// PlanToAst test cases ////////////////////////////
    
    /**
     * Tests the PlanToAst test cases end-to-end.
     * 
     * @throws Exception
     *             if something goes wrong
     */
    public void testPlanToAstTestCases() throws Exception
    {
        verify(TestPlanToAstTranslator.class, "TestPlanToAstTranslator-testTerm", true);
        verify(TestPlanToAstTranslator.class, "TestPlanToAstTranslator-testSelectAll", true);
        verify(TestPlanToAstTranslator.class, "TestPlanToAstTranslator-testInnerJoin", true);
        verify(TestPlanToAstTranslator.class, "TestPlanToAstTranslator-testNestedJoin", true);
        verify(TestPlanToAstTranslator.class, "TestPlanToAstTranslator-testProduct", true);
        verify(TestPlanToAstTranslator.class, "TestPlanToAstTranslator-testProject", true);
        verify(TestPlanToAstTranslator.class, "TestPlanToAstTranslator-testSelect", true);
        verify(TestPlanToAstTranslator.class, "TestPlanToAstTranslator-testEliminateDuplicates", true);
        verify(TestPlanToAstTranslator.class, "TestPlanToAstTranslator-testOuterJoin", true);
        verify(TestPlanToAstTranslator.class, "TestPlanToAstTranslator-testSendPlan", true);
        verify(TestPlanToAstTranslator.class, "TestPlanToAstTranslator-testUnion", true);
        verify(TestPlanToAstTranslator.class, "TestPlanToAstTranslator-testUnion2", true);
        verify(TestPlanToAstTranslator.class, "TestPlanToAstTranslator-testIntersect", true);
        verify(TestPlanToAstTranslator.class, "TestPlanToAstTranslator-testExcept", true);
        verify(TestPlanToAstTranslator.class, "TestPlanToAstTranslator-testSort", true);
        verify(TestPlanToAstTranslator.class, "TestPlanToAstTranslator-testOffsetFetch", true);
        verify(TestPlanToAstTranslator.class, "TestPlanToAstTranslator-testOffsetFetch2", true);
        verify(TestPlanToAstTranslator.class, "TestPlanToAstTranslator-testGroupBy", true);
        verify(TestPlanToAstTranslator.class, "TestPlanToAstTranslator-testGroupByHavingCond", true);
    }
    
    // //////////////////////// Regression test cases ////////////////////////////////////
    
    /**
     * Tests what the LogicalPlanBuilder produces for queries that involve complex data objects.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testDmlNestedCollection() throws Exception
    {
        verify("TestEndToEnd-testDeleteNestedCollection", true);
        verify("TestEndToEnd-testUpdateNestedCollection", true);
        verify("TestEndToEnd-testInsertNestedCollection", true);
    }
    
    /**
     * Tests what the LogicalPlanBuilder produces for queries that involve complex data objects.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testComplex() throws Exception
    {
        verify("TestEndToEnd-testScanComplex", true);
    }
    
    /**
     * Tests a temporary query expression.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testTemp() throws Exception
    {
        verify("TestEndToEnd-testTemp", true);
    }
    
    /**
     * Tests deep navigation inside tuples.
     * 
     * @throws Exception
     *             if something goes wrong
     */
    public void testNavigateIntoTupleInInnerQuery() throws Exception
    {
        verify("TestEndToEnd-testNavigateIntoTupleInInnerQuery", true);
        verify("TestEndToEnd-testNavigateIntoTupleInInnerQueryStar", true);
        verify("TestEndToEnd-testNavigateIntoTupleInInnerQueryStar2", true);
    }
    
    /**
     * Tests delete from JDBC with a condition involving casting a tuple to string.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    @Test(groups = AbstractTestCase.FAILURE)
    // due to KeyInference
    public void testDeleteTupleString() throws Exception
    {
        verify("TestEndToEnd-testDeleteTupleString", false);
    }
    
    /**
     * Tests a simple distributed query.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testSimple() throws Exception
    {
        verify("TestEndToEnd-testSimpleDistributedQuery", true);
    }
    
    /**
     * Tests inline switch.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testInlineSwitch() throws Exception
    {
        verify("TestEndToEnd-testInlineSwitch3", true);
        verify("TestEndToEnd-testInlineSwitch7", true);
    }
    
    /**
     * Tests tuple all item.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testTupleAllItem() throws Exception
    {
        verify("TestEndToEnd-testTupleAllItem", true);
    }
    
    /**
     * Tests reference to tuple variable.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testTupleVariable() throws Exception
    {
        verify("TestEndToEnd-testTupleVariableAbsoluteVar", true);
        verify("TestEndToEnd-testTupleVariableSubquery", true);
    }
    
    /**
     * Tests that the AT keyword is not accepted when the input collection is not ordered.
     * 
     * @throws Exception
     *             if something goes wrong
     */
    @Test(groups = FAILURE)
    // Disabled because now we allow AT keyword on non-ordered collections
    public void testListsError() throws Exception
    {
        try
        {
            verify("TestEndToEnd-testListsOutputError", true);
        }
        catch (Exception e)
        {
            assertEquals(QueryCompilation.INPUT_ORDER_INPUT_IS_NOT_ORDERED.getMessage(), e.getMessage());
        }
    }
    
    /**
     * Tests a total aggregation pushed to postgres.
     * 
     * @throws Exception
     *             if something goes wrong
     */
    public void testD88() throws Exception
    {
        verify("TestEndToEnd-testD88", true);
    }
    
    /**
     * Tests variable resolution with identical alias in a subquery.
     * 
     * @throws Exception
     *             if something goes wrong
     */
    public void testD165() throws Exception
    {
        verify("TestEndToEnd-testD165", true);
    }
    
    /**
     * Tests variable resolution in subqueries.
     * 
     * @throws Exception
     *             if something goes wrong
     */
    public void testD245() throws Exception
    {
        verify("TestEndToEnd-testD245", true);
    }
    
    /**
     * Tests union and subqueries.
     * 
     * @throws Exception
     *             if something goes wrong
     */
    public void testD248() throws Exception
    {
        verify("TestEndToEnd-testD248", true);
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
        verify(this.getClass(), relative_file_name, check_output);
    }
    
    /**
     * Verifies the output of the query processor.
     * 
     * @param relative_file_name
     *            the relative path of the XML file specifying the test case.
     * @param check_output
     *            whether to check the output of the query.
     * @param clazz
     *            the class of the module that is tested
     * @throws Exception
     *             if an error occurs.
     */
    protected void verify(Class<?> clazz, String relative_file_name, boolean check_output) throws Exception
    {
        // Parse the test case from the XML file
        parseTestCase(clazz, relative_file_name + ".xml");
        
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        String query_expr = getQueryExpression(0);
        
        List<AstTree> ast_trees = qp.parseQuery(query_expr, new LocationImpl(relative_file_name + ".xml"));
        
        for (AstTree ast_tree : ast_trees)
        {
            qp.setUnifiedApplicationState(uas);
            
            LogicalPlan logical_plan = null;
            PhysicalPlan physical_plan = null;
            
            // Construct the logical plan
            logical_plan = qp.translate(Collections.singletonList(ast_tree), uas).get(0);
            
            // Check against normal form
            new InitialNormalFormChecker(uas).check(logical_plan);
            
            // Rewrite the logical plan
            logical_plan = qp.rewriteSourceAgnostic(Collections.singletonList(logical_plan), uas).get(0);
            
            // Distribute the logical plans to distributed normal form
            logical_plan = qp.distribute(Collections.singletonList(logical_plan), uas).get(0);
            
            // Apply source-specific rewritings
            logical_plan = qp.rewriteSourceSpecific(Collections.singletonList(logical_plan), uas).get(0);
            
            // Generate the physical plans
            physical_plan = qp.generate(Collections.singletonList(logical_plan), uas).get(0);
            
            // Cleanup
            qp.cleanup(false);
            
            if (DEBUG) logLogicalPlanExplain(physical_plan.getLogicalPlan());
            if (DEBUG) logLogicalPlanXml(physical_plan.getLogicalPlan());
            
            // Check the logical plan
            ConsistencyChecker.getInstance(uas).check(physical_plan.getLogicalPlan());
            DistributedNormalFormChecker.getInstance(uas).check(physical_plan.getLogicalPlan());
            
            // Check the output value
            Value output_value = qp.createEagerQueryResult(physical_plan, uas).getValue();
            if (DEBUG) logOutputValueExplain(output_value);
            if (DEBUG) logOutputValueXml(output_value);
            if (DEBUG) logOutputType(physical_plan.getLogicalPlan().getOutputType());
            if (check_output) checkOutputValue(output_value, uas);
        }
    }
}
