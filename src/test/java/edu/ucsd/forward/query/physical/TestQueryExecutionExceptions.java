/**
 * 
 */
package edu.ucsd.forward.query.physical;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.type.TypeException;
import edu.ucsd.forward.exception.ExceptionMessages;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.cast.CastFunction;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * Tests query execution errors.
 * 
 * @author Michalis Petropoulos
 * 
 */
@Test
public class TestQueryExecutionExceptions extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestQueryExecutionExceptions.class);
    
    /**
     * Tests a query execution exception message.
     * 
     * FIXME: Yupeng: we mark this test case as failure, because hudson runs an old version of postgres which does not show the
     * detail of the sql exception message.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testJdbcError() throws Exception
    {
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), "TestQueryExecutionExceptions-testJdbcError.xml");
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        PhysicalPlan physical_plan = getPhysicalPlan(0);
        
        String actual = "";
        try
        {
            // Catch the exception thrown when the first physical plan is evaluated
            checkPlanEvaluation(physical_plan, uas);
        }
        catch (QueryExecutionException ex)
        {
            actual = ex.getSingleLineMessageWithLocation();
        }
        
        // JDBC_STMT_EXEC_ERROR
        QueryExecutionException ex = new QueryExecutionException(
                                                                 QueryExecution.JDBC_STMT_EXEC_ERROR,
                                                                 "db",
                                                                 "INSERT INTO public.customers(cid, name) VALUES (?, ?)",
                                                                 "Batch entry 0 INSERT INTO public.customers(cid, name) VALUES ('1', 'John') was aborted.  Call getNextException to see the cause.ERROR: duplicate key value violates unique constraint \"customers_pkey\"\n  Detail: Key (cid)=(1) already exists.");
        String expected = ex.getSingleLineMessage();
        expected = IoUtil.normalizeLineBreak(expected);
        actual = IoUtil.normalizeLineBreak(actual);
        assertEquals(expected, actual);
        
        // Check that the second physical plan is evaluated correctly
        physical_plan = getPhysicalPlan(1);
        checkPlanEvaluation(physical_plan, uas);
    }
    
    /**
     * Tests a query execution exception message.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testFunctionEvalError() throws Exception
    {
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), "TestQueryExecutionExceptions-testFunctionEvalError.xml");
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        PhysicalPlan physical_plan = getPhysicalPlan(0);
        
        String actual = "";
        try
        {
            // Catch the exception thrown when the first physical plan is evaluated
            checkPlanEvaluation(physical_plan, uas);
        }
        catch (QueryExecutionException ex)
        {
            actual = ex.getSingleLineMessageWithLocation();
        }
        
        // FUNCTION_EVAL_ERROR
        Throwable cause = new TypeException(ExceptionMessages.Type.INVALID_STRING_TO_TYPE_CONVERION, "abc",
                                            TypeEnum.INTEGER.getName());
        QueryExecutionException ex = new QueryExecutionException(QueryExecution.FUNCTION_EVAL_ERROR, cause, CastFunction.NAME);
        String expected = ex.getSingleLineMessage();
        assertEquals(expected, actual);
        
        // Check that the second physical plan is evaluated correctly
        physical_plan = getPhysicalPlan(1);
        checkPlanEvaluation(physical_plan, uas);
    }    
}
