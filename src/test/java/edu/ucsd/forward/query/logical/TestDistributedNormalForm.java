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
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * Test the distributed normal form.
 * 
 * @author Vicky Papavasileiou
 * @author Michalis Petropoulos
 * @author Romain Vernoux
 */
@Test
public class TestDistributedNormalForm extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger  log   = Logger.getLogger(TestJoinOrdering.class);
    
    /**
     * If this flag is enabled, the testcases log information useful for creating/debugging test cases.
     */
    private static final boolean DEBUG = false;
    
    public void testJoinDbJoinMediator() throws Exception
    {
        verify("TestDistributedNormalForm-testJoinDbJoinMediator");
    }
    
    public void testOuterJoinDbJoinMediator() throws Exception
    {
        verify("TestDistributedNormalForm-testOuterJoinDbJoinMediator");
    }
    
    public void testScanWithoutNavigatesWithProduct() throws Exception
    {
        verify("TestDistributedNormalForm-testScanWithoutNavigates");
    }
    
    public void testProjectWithConstants() throws Exception
    {
        verify("TestDistributedNormalForm-testProjectWithConstants");
    }
    
    public void testProjectAboveGround() throws Exception
    {
        verify("TestDistributedNormalForm-testProjectAboveGround");
    }
    
    public void testProjectAboveGroundWithFunction() throws Exception
    {
        verify("TestDistributedNormalForm-testProjectAboveGround-WithFunction");
    }
    
    /**
     * Fails in Hudson because it has Postgres 8.3 Requires Postgres 8.4 and above.
     * 
     * @throws Exception
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testProjectAboveGroundWithExternalFunction() throws Exception
    {
        JdbcDataSource data_source = null;
        try
        {
            data_source = setupJdbcDataSource(this.getClass(), "TestAstToPlanTranslator-testExternalFunctionSetup",
                                              "TestAstToPlanTranslator-testExternalFunctionSetup");
            verify("TestDistributedNormalForm-testProjectAboveGround-WithExternalFunction");
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
    
    public void testSelectWithConstantsAboveScan() throws Exception
    {
        verify("TestDistributedNormalForm-testSelectWithConstantsAboveScan");
    }
    
    public void testSelectWithConstantsAboveGround() throws Exception
    {
        verify("TestDistributedNormalForm-testSelectWithConstantsAboveGround");
    }
    
    public void testFunctionDistributedArguments1() throws Exception
    {
        verify("TestDistributedNormalForm-testFunctionDistributedArguments1");
    }
    
    /* 2. multiple functions where one is not SQL compliant */// Not working
    @Test(groups = AbstractTestCase.FAILURE)
    public void testFunctionDistributedArguments2() throws Exception
    {
        verify("TestDistributedNormalForm-testFunctionDistributedArguments2");
    }
    
    /* 3. The argument of a non-SQL compliant is from db */// Not working
    @Test(groups = AbstractTestCase.FAILURE)
    public void testFunctionDistributedArguments3() throws Exception
    {
        verify("TestDistributedNormalForm-testFunctionDistributedArguments2");
    }
    
    public void testCaseFunctionDistributedArguments() throws Exception
    {
        verify("TestDistributedNormalForm-testCaseFunctionDistributedArguments");
    }
    
    /**
     * Tests the annotation with execution sites of DML operators. 1. Insert 2. Update 3. Delete.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testDml() throws Exception
    {
        verify("TestDistributedNormalForm-testInsert");
        verify("TestDistributedNormalForm-testDelete");
        verify("TestDistributedNormalForm-testUpdate");
    }
    
    public void testOrderByMediatorFunction() throws Exception
    {
        verify("TestDistributedNormalForm-testOrderByMediatorFunction");
    }
    
    /**
     * Tests the annotation with execution sites of external functions.
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
            
            verify("TestAstToPlanTranslator-testExternalFunctionScalar");
            verify("TestAstToPlanTranslator-testExternalFunctionScalarComposition");
            verify("TestAstToPlanTranslator-testExternalFunctionScalarSelect");
            verify("TestAstToPlanTranslator-testExternalFunctionTuple");
            verify("TestAstToPlanTranslator-testExternalFunctionTupleSelect");
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
     * Verifies if a logical plan is in distributed normal form.
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
        
        if (DEBUG)
        {
            // Get the original query string
            String query_expr = getQueryExpression(0);
            List<AstTree> ast_trees = qp.parseQuery(query_expr, new LocationImpl(relative_file_name + ".xml"));
            assert (ast_trees.size() == 1);
            
            // Translate the AST
            LogicalPlan original_plan = qp.translate(Collections.singletonList(ast_trees.get(0)), uas).get(0);
            
            // Logs the plan
            logLogicalPlanExplain(original_plan);
            logLogicalPlanXml(original_plan);
        }
        
        LogicalPlan logical_plan = getLogicalPlan(0);
        
        // Check that the logical plan can reach distributed normal form
        LogicalPlan actual = qp.distribute(Collections.singletonList(logical_plan), uas).get(0);
        
        if (DEBUG)
        {
            logLogicalPlanExplain(actual);
            logLogicalPlanXml(actual);
        }
        ConsistencyChecker.getInstance(uas).check(actual);
        DistributedNormalFormChecker.getInstance(uas).check(actual);
        
        // Check that the actual logical plan can be converted to physical plan
        qp.generate(Collections.singletonList(actual), uas).get(0);
        
        LogicalPlan expected = getLogicalPlan(1);
        
        checkLogicalPlan(actual, expected, uas);
        
        // Make sure a physical can be generated and executed
        PhysicalPlan physical_plan = qp.generate(Collections.singletonList(actual), uas).get(0);
        Value result = qp.createEagerQueryResult(physical_plan, uas).getValue();
        
        if (DEBUG)
        {
            logOutputValueXml(result);
        }
    }
    
}
