/**
 * 
 */
package edu.ucsd.forward.query.suspension;

import java.util.Collections;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.CheckedException;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.EagerQueryResult;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * Tests the support suspension in the operator implementation.
 * 
 * @author Yupeng
 * 
 */
@Test
public class TestOperatorSuspension extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestOperatorSuspension.class);
    
    // /**
    // * Tests the implementation of scan idb.
    // *
    // * @throws Exception
    // * when encounters error
    // */
    // public void testScanImpl() throws Exception
    // {
    // checkOperatorSuspension("TestOperatorSuspension-testScanImplIdb");
    // }
    
    /**
     * Tests the implementation of project.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testProjectImpl() throws Exception
    {
        checkOperatorSuspension("TestOperatorSuspension-testProjectImpl");
    }
    
    /**
     * Tests the implementation of duplicates elimination.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testEliminateDuplicatesImpl() throws Exception
    {
        checkOperatorSuspension("TestOperatorSuspension-testEliminateDuplicatesImpl");
    }
    
    /**
     * Tests the implementation of inner join using nested loops.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testInnerJoinImplNestedLoop() throws Exception
    {
        checkOperatorSuspension("TestOperatorSuspension-testInnerJoinImplNestedLoopList");
    }
    
    /**
     * Tests the implementation of full outer join implementation using nested loops.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testFullOuterJoinImplNestedLoop() throws Exception
    {
        checkOperatorSuspension("TestOperatorSuspension-testFullOuterJoinImplNestedLoop");
    }
    
    /**
     * Tests the implementation of left outer join implementation using nested loops.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testLeftOuterJoinImplNestedLoop() throws Exception
    {
        checkOperatorSuspension("TestOperatorSuspension-testLeftOuterJoinImplNestedLoopHash");
    }
    
    /**
     * Tests the implementation of right outer join implementation using nested loops.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testRightOuterJoinImplNestedLoop() throws Exception
    {
        checkOperatorSuspension("TestOperatorSuspension-testRightOuterJoinImplNestedLoopHash");
    }
    
    /**
     * Tests the implementation of semi-join implementation using nested loops.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testSemiJoinImplNestedLoop() throws Exception
    {
        checkOperatorSuspension("TestOperatorSuspension-testSemiJoinImplNestedLoopHash");
    }
    
    /**
     * Tests the implementation of anti-semi-join implementation using nested loops.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testAntiSemiJoinImplNestedLoop() throws Exception
    {
        checkOperatorSuspension("TestOperatorSuspension-testAntiSemiJoinImplNestedLoopHash");
    }
    
    /**
     * Tests the implementation of product by nested loop.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testProductImplNestedLoop() throws Exception
    {
        checkOperatorSuspension("TestOperatorSuspension-testProductImplNestedLoop");
    }
    
    /**
     * Tests the implementation of select by sequential scanning.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testSelectImplSeq() throws Exception
    {
        checkOperatorSuspension("TestOperatorSuspension-testSelectImplSeq");
    }
    
    /**
     * Tests the implementation of group by and aggregate functions.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testGroupByImpl() throws Exception
    {
        checkOperatorSuspension("TestOperatorSuspension-testGroupByImplAll");
        checkOperatorSuspension("TestOperatorSuspension-testGroupByImplDistinct");
    }
    
    /**
     * Tests the implementation of except.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testExceptImpl() throws Exception
    {
        checkOperatorSuspension("TestOperatorSuspension-testExceptImpl");
        checkOperatorSuspension("TestOperatorSuspension-testExceptImplAll");
    }
    
    /**
     * Tests the implementation of intersect.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testIntersectImpl() throws Exception
    {
        checkOperatorSuspension("TestOperatorSuspension-testIntersectImpl");
        checkOperatorSuspension("TestOperatorSuspension-testIntersectImplAll");
    }
    
    /**
     * Tests the implementation of union.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testUnionImpl() throws Exception
    {
        checkOperatorSuspension("TestOperatorSuspension-testUnionImpl");
        checkOperatorSuspension("TestOperatorSuspension-testUnionImplAll");
    }
    
    /**
     * Tests the implementation of sort.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testSortImpl() throws Exception
    {
        checkOperatorSuspension("TestOperatorSuspension-testSortImpl");
    }
    
    /**
     * Tests the implementation of offset fetch.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testOffsetFetchImpl() throws Exception
    {
        checkOperatorSuspension("TestOperatorSuspension-testOffsetFetchImpl");
    }
    
    /**
     * Tests the implementation of apply plan.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testApplyPlanImpl() throws Exception
    {
        checkOperatorSuspension("TestOperatorSuspension-testApplyPlanImpl");
        checkOperatorSuspension("TestOperatorSuspension-testApplyPlanImplNested");
    }
    
    /**
     * Tests the implementation of exists.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testExistsImpl() throws Exception
    {
        checkOperatorSuspension("TestOperatorSuspension-testExistsImpl");
    }
    
    /**
     * Tests the implementation of the index scan operator.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testIndexScan() throws Exception
    {
        checkOperatorSuspension("TestOperatorSuspension-testIndexScan");
    }
    
    /**
     * Tests the output of the physical plan meets the expectation.
     * 
     * @param relative_file_name
     *            the relative path of the XML file specifying the test case.
     * @throws Exception
     *             if an error occurs.
     */
    private void checkOperatorSuspension(String relative_file_name) throws Exception
    {
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), relative_file_name + ".xml");
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        PhysicalPlan expected = getPhysicalPlan(0);
        
        LogicalPlan logical_plan = getLogicalPlan(0);
        
        // Check if the logical plan can be copied
        LogicalPlan logical_copy = logical_plan.copy();
        
        // Check if the copied logical plan is the same as the initial
        checkLogicalPlan(logical_plan, logical_copy, uas);
        
        // Check that the actual logical plan can be converted to physical plan
        PhysicalPlan actual = QueryProcessorFactory.getInstance().generate(Collections.singletonList(logical_plan), uas).get(0);
        
        // Check the logical to physical plan translation
        checkPhysicalPlan(actual, expected);
        
        // Check if the physical plan is evaluated correctly
        checkPlanEvaluation(expected, uas);
        
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
        EagerQueryResult result = QueryProcessorFactory.getInstance().createEagerQueryResult(physical_plan, uas);
        Value query_result = process(result);
        
        DataTree actual_output = new DataTree(query_result);
        
        DataTree expected_output = uas.getDataSource(OUTPUT_ELM).getDataObject(OUTPUT_ELM);
        
        AbstractTestCase.assertEqualSerialization(expected_output.getRootValue(), actual_output.getRootValue());
        
        // FIXME Check that the schema trees are isomorphic
        
        return query_result;
    }
    
    /**
     * Processes the eager query execution with the suspension handling.
     * 
     * @param result
     *            the eager query result during execution.
     * @return the executed result.
     * @throws QueryExecutionException
     *             if an error occurs during query execution.
     */
    private static Value process(EagerQueryResult result) throws QueryExecutionException
    {
        try
        {
            return result.getValueWithSuspension();
        }
        catch (SuspensionException e)
        {
            SuspensionRequest request = e.getRequest();
            SuspensionHandler handler = SuspensionRequestDispatcher.getInstance().dispatch(request);
            handler.handle();
            
            return process(result);
        }
    }
}
