/**
 * 
 */
package edu.ucsd.forward.query.suspension;

import java.util.Collections;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceException;
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
 * Tests the support suspension in the DML operator implementation.
 * 
 * @author Yupeng
 * 
 */
@Test
public class TestOperatorDmlSuspension extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log          = Logger.getLogger(TestOperatorDmlSuspension.class);
    
    private static final String EXPECTED_ELM = "expected";
    
    private static final String ACTUAL_ELM   = "actual";
    
    /**
     * Tests the implementation of insert.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testInsert() throws Exception
    {
        checkOperatorSuspension("TestOperatorDmlSuspension-testInsert");
        checkOperatorSuspension("TestOperatorDmlSuspension-testInsertWithColumnList");
    }
    
    /**
     * Tests the implementation of delete.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testDelete() throws Exception
    {
        checkOperatorSuspension("TestOperatorDmlSuspension-testDeleteIdb");
        checkOperatorSuspension("TestOperatorDmlSuspension-testDeleteIdbSubQueryIdb");
        checkOperatorSuspension("TestOperatorDmlSuspension-testDeleteIdbSubQuery");
    }
    
    /**
     * Tests the implementation of update.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testUpdate() throws Exception
    {
        checkOperatorSuspension("TestOperatorDmlSuspension-testUpdateIdb");
        checkOperatorSuspension("TestOperatorDmlSuspension-testUpdateIdbSubQuery");
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
        
        // Execute the plan
        checkPlanEvaluation(expected, uas);
        
        // Check if the DML execution result is correct
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
        
        AbstractTestCase.assertEqualSerialization(expected_output.getRootValue(), actual_output.getRootValue());
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
