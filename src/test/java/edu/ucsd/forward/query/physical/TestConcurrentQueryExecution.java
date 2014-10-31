/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.CheckedException;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * Tests that the query processor is capable of concurrently executing multiple queries.
 * 
 * @author Michalis Petropoulos
 * 
 */
@Test(groups = AbstractTestCase.PERFORMANCE)
public class TestConcurrentQueryExecution extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestConcurrentQueryExecution.class);
    
    /**
     * Tests a query execution.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testFlat() throws Exception
    {
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), "TestConcurrentQueryExecution-TestFlat.xml");
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        List<AstTree> ast_trees = QueryProcessorFactory.getInstance().parseQuery(getQueryExpression(0), new LocationImpl());
        PhysicalPlan physical_plan = QueryProcessorFactory.getInstance().compile(ast_trees, uas).get(0);
        
        // Execute the plan
        Value query_result = QueryProcessorFactory.getInstance().createEagerQueryResult(physical_plan, uas).getValue();
        DataTree actual_output = new DataTree(query_result);
        
        uas.getDataSource(OUTPUT_ELM).setDataObject(OUTPUT_ELM, actual_output);
        
        int num_of_threads = 10;
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < num_of_threads; i++)
        {
            threads.add(new Thread(new QueryExecutionThread(physical_plan, uas), String.valueOf(i)));
        }
        
        // Check that the threads execute correctly
        for (int i = 0; i < num_of_threads; i++)
        {
            threads.get(i).start();
        }
        
        Thread.sleep(30000);
    }
    
    /**
     * Tests a query execution.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testNested() throws Exception
    {
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), "TestConcurrentQueryExecution-TestNested.xml");
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        List<AstTree> ast_trees = QueryProcessorFactory.getInstance().parseQuery(getQueryExpression(0), new LocationImpl());
        PhysicalPlan physical_plan = QueryProcessorFactory.getInstance().compile(ast_trees, uas).get(0);
        
        // Execute the plan
        Value query_result = QueryProcessorFactory.getInstance().createEagerQueryResult(physical_plan, uas).getValue();
        DataTree actual_output = new DataTree(query_result);
        
        uas.getDataSource(OUTPUT_ELM).setDataObject(OUTPUT_ELM, actual_output);
        
        int num_of_threads = 10;
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < num_of_threads; i++)
        {
            threads.add(new Thread(new QueryExecutionThread(physical_plan, uas), String.valueOf(i)));
        }
        
        // Check that the threads execute correctly
        for (int i = 0; i < num_of_threads; i++)
        {
            threads.get(i).start();
        }
        
        Thread.sleep(30000);
    }
    
    private static class QueryExecutionThread implements Runnable
    {
        private PhysicalPlan            m_plan;
        
        private UnifiedApplicationState m_uas;
        
        public QueryExecutionThread(PhysicalPlan plan, UnifiedApplicationState uas)
        {
            m_plan = plan;
            m_uas = uas;
        }
        
        public void run()
        {
            String thread_name = Thread.currentThread().getName();
            System.out.format("%s: %s%n", thread_name, "START");
            
            try
            {
                checkPlanEvaluation(m_plan, m_uas);
            }
            catch (CheckedException e)
            {
                System.out.format("%s: %s%n", thread_name, e.getSingleLineMessage());
            }
            
            System.out.format("%s: %s%n", thread_name, "END");
        }
    }
    
}
