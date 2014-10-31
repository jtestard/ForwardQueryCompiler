/**
 * 
 */
package edu.ucsd.forward.experiment.set_processable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;
import org.testng.v6.Lists;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;

/**
 * Tests varying the join selectivity.
 * 
 * @author Kian Win Ong
 * 
 */
@Test(groups = edu.ucsd.forward.test.AbstractTestCase.PERFORMANCE)
public class TestVarySelectivity extends AbstractSetProcessableTestCase
{
    private static final Logger        log                               = Logger.getLogger(TestVarySelectivity.class);
    
    private static final String        APPLY_PLAN_FILE_NAME              = "TestVaryDisplayedNations-ApplyPlan.xml";
    
    private static final String        NORMALIZED_SETS_FILE_NAME         = "TestVaryDisplayedNations-NormalizedSets.xml";
    
    private static final String        EXPLAIN_APPLY_PLAN_FILE_NAME      = "TestVarySelectivity-Explain-apply-plan-new.sql";
    
    private static final String        EXPLAIN_NORMALIZED_SETS_FILE_NAME = "TestVarySelectivity-Explain-normalized-sets-new.sql";
    
    private static final String        GENERATE_FILE_NAME                = "TestVarySelectivity-Generate.sql";
    
    private static final String        SELECTED_NATIONS_FILE_NAME        = "selected-nations-10.xml";
    
    private static final String        QUERY_FILE_NAME                   = "TestVaryDisplayedNations-Query.xml";
    
    private static final List<Integer> TOTAL_NATIONS                     = new ArrayList<Integer>();
    
    static
    {
        TOTAL_NATIONS.add(10);
        TOTAL_NATIONS.add(20);
        TOTAL_NATIONS.add(40);
        TOTAL_NATIONS.add(100);
        TOTAL_NATIONS.add(200);
        TOTAL_NATIONS.add(400);
        TOTAL_NATIONS.add(1000);
        TOTAL_NATIONS.add(2000);
        TOTAL_NATIONS.add(4000);
    }
    
    private static final String        JDBC_CONNECTION;
    
    static
    {
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:postgresql://");
        sb.append("localhost:9000/");
        sb.append("tpch3_1200?");
        sb.append("user=ubuntu&");
        sb.append("password=ubuntu");
        JDBC_CONNECTION = sb.toString();
    }
    
    private static final Integer       DISCARD                           = 1;
    
    private static final Integer       REPEAT                            = 3;
    
    /**
     * Tests the apply-plan logical plan.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    public void testApplyPlan() throws Exception
    {
        run(QUERY_FILE_NAME, false);
    }
    
    /**
     * Tests the apply-plan logical plan.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    public void testApplyPlanOld() throws Exception
    {
        run(APPLY_PLAN_FILE_NAME);
    }
    
    /**
     * Tests the normalized-sets logical plan.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    public void testNormalizedSetsOld() throws Exception
    {
        run(NORMALIZED_SETS_FILE_NAME);
    }
    
    /**
     * Tests the normalized-sets logical plan.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    public void testNormalizedSets() throws Exception
    {
        run(QUERY_FILE_NAME, true);
    }
    
    /**
     * Runs EXPLAIN to check the Postgresql plan for respective SQL queries.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    public void testExplain() throws Exception
    {
//        explain(EXPLAIN_NORMALIZED_SETS_FILE_NAME);
        explain(EXPLAIN_APPLY_PLAN_FILE_NAME);
    }
    
    /**
     * Runs the Postgresql statement for respective SQL queries.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    public void testExecuteStatement() throws Exception
    {
        // executeStatement(EXPLAIN_NORMALIZED_SETS_FILE_NAME);
        executeStatement(EXPLAIN_APPLY_PLAN_FILE_NAME);
    }
    
    /**
     * Generates the corresponding <code>nations_x000</code> and <code>customers_x000</code> table for each TOTAL_NATION.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    public void testGenerate() throws Exception
    {
        generate();
    }
    
    /**
     * Runs the logical plan.
     * 
     * @param query_file_name
     *            the file name of the logical plan.
     * @param rewrite
     *            whether to rewrite the apply-plan
     * @throws Exception
     *             if an exception occurs.
     */
    private static void run(String query_file_name, boolean rewrite) throws Exception
    {
        List<Object[]> results = Lists.newArrayList();
        for (Integer total_nations : TOTAL_NATIONS)
        {
            Map<String, String> rename_variables = new LinkedHashMap<String, String>();
            rename_variables.put("nation", "nations_x" + total_nations);
            rename_variables.put("customers", "customers_x" + total_nations);
            
            double sum = 0.0;
            
            for (int i = 0; i < DISCARD; i++)
            {
                executeEndToEnd(query_file_name, SELECTED_NATIONS_FILE_NAME, rename_variables, rewrite);
            }
            
            for (int i = 0; i < REPEAT; i++)
            {
                sum += executeEndToEnd(query_file_name, SELECTED_NATIONS_FILE_NAME, rename_variables, rewrite);
            }
            
            double average = sum / REPEAT;
            results.add(new Object[] { query_file_name, total_nations, average });
            log.info("{} {} {}", new Object[] { query_file_name, total_nations, average });
        }
        log.info("Results:");
        for (Object[] result : results)
        {
            log.info("{} {} {}", result);
        }
    }
    
    /**
     * Runs the logical plan.
     * 
     * @param logical_file_name
     *            the file name of the logical plan.
     * @throws Exception
     *             if an exception occurs.
     */
    private static void run(String logical_file_name) throws Exception
    {
        List<Object[]> results = Lists.newArrayList();
        for (Integer total_nations : TOTAL_NATIONS)
        {
            Map<String, String> rename_variables = new LinkedHashMap<String, String>();
            rename_variables.put("nation", "nations_x" + total_nations);
            rename_variables.put("customers", "customers_x" + total_nations);
            
            double sum = 0.0;
            
            for (int i = 0; i < DISCARD; i++)
            {
                execute(logical_file_name, SELECTED_NATIONS_FILE_NAME, rename_variables);
            }
            
            for (int i = 0; i < REPEAT; i++)
            {
                sum += execute(logical_file_name, SELECTED_NATIONS_FILE_NAME, rename_variables);
            }
            
            double average = sum / REPEAT;
            results.add(new Object[] { logical_file_name, total_nations, average });
            log.info("{} {} {}", new Object[] { logical_file_name, total_nations, average });
        }
        log.info("Results:");
        for (Object[] result : results)
        {
            log.info("{} {} {}", result);
        }
    }
    
    /**
     * Runs EXPLAIN to check the Postgresql plan for the given SQL query.
     * 
     * @param sql_file_name
     *            the file name for the SQL query.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    private static void explain(String sql_file_name) throws Exception
    {
        String explain = IoUtil.getResourceAsString(AbstractSetProcessableTestCase.class, sql_file_name);
        
        for (Integer total_nations : TOTAL_NATIONS)
        {
            Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection(JDBC_CONNECTION);
            
            String explain_instance = explain.replaceAll("customers_000", "customers_x" + total_nations);
            Statement statement = connection.createStatement();
            statement.execute(explain_instance);
            StringBuilder output = new StringBuilder();
            do
            {
                ResultSet result_set = statement.getResultSet();
                if (result_set != null)
                {
                    while (result_set.next())
                    {
                        output.append(result_set.getString(1));
                        output.append("\n");
                    }
                    result_set.close();
                }
            } while (statement.getMoreResults() || statement.getUpdateCount() != -1);
            statement.close();
            log.info("{} {}\n{}", new Object[] { sql_file_name, total_nations, output });
            
            connection.close();
        }
        
    }
    
    /**
     * Runs to check the Postgresql execution time for the given SQL query.
     * 
     * @param sql_file_name
     *            the file name for the SQL query.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    private static void executeStatement(String sql_file_name) throws Exception
    {
        String explain = IoUtil.getResourceAsString(AbstractSetProcessableTestCase.class, sql_file_name);
        List<Object[]> results = Lists.newArrayList();
        for (Integer total_nations : TOTAL_NATIONS)
        {
            String explain_instance = explain.replaceAll("customers_000", "customers_x" + total_nations);
            String sql = explain_instance.replaceAll("explain", "");
            
            double sum = 0.0;
            
            for (int i = 0; i < DISCARD; i++)
            {
                measureStatement(sql);
            }
            
            for (int i = 0; i < REPEAT; i++)
            {
                sum += measureStatement(sql);
            }
            
            double average = sum / REPEAT;
            results.add(new Object[] { sql_file_name, total_nations, average });
            log.info("{} {} {}", new Object[] { sql_file_name, total_nations, average });
        }
        log.info("Results:");
        for (Object[] result : results)
        {
            log.info("{} {} {}", result);
        }
    }
    
    private static double measureStatement(String sql) throws Exception
    {
        Class.forName("org.postgresql.Driver");
        Connection connection = DriverManager.getConnection(JDBC_CONNECTION);
        Statement statement = connection.createStatement();
        long start = System.currentTimeMillis();
        statement.execute(sql);
        long end = (System.currentTimeMillis() - start);
        StringBuilder output = new StringBuilder();
        do
        {
            ResultSet result_set = statement.getResultSet();
            if (result_set != null)
            {
                while (result_set.next())
                {
                    output.append(result_set.getString(1));
                    output.append("\n");
                }
                result_set.close();
            }
        } while (statement.getMoreResults() || statement.getUpdateCount() != -1);
        statement.close();
        log.info(output.toString());
        
        connection.close();
        return end;
    }
    
    /**
     * Generates the corresponding <code>nations_x000</code> and <code>customers_x000</code> table for each TOTAL_NATION.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    private static void generate() throws Exception
    {
        String generate = IoUtil.getResourceAsString(AbstractSetProcessableTestCase.class, GENERATE_FILE_NAME);
        
        for (Integer total_nations : TOTAL_NATIONS)
        {
            Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection(JDBC_CONNECTION);
            
            String generate_instance = generate.replaceAll("000", total_nations + "");
            log.info(generate_instance);
            Statement statement = connection.createStatement();
            statement.execute(generate_instance);
            statement.close();
            connection.close();
        }
    }
}
