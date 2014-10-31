/**
 * 
 */
package edu.ucsd.forward.experiment.set_processable_new;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import org.testng.annotations.Test;
import org.testng.v6.Lists;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;

/**
 * @author Yupeng Fu
 */
@Test(groups = edu.ucsd.forward.test.AbstractTestCase.PERFORMANCE)
public class TestVaryFanout extends AbstractSetProcessableTestCaseNew
{
    private static final Logger  log                          = Logger.getLogger(TestVaryFanout.class);
    
    private static final String  NS_FILE_NAME                 = "TestVaryFanout-NS.xml";
    
    private static final String  EXPLAIN_AP_FILE_NAME = "TestVaryFanout-Explain-AP.sql";
    
    private static final String  EXPLAIN_NS_FILE_NAME = "TestVaryFanout-Explain-NS.sql";
    
    private static final String  GENERATE_FILE_NAME           = "TestVaryFanout-Generate.sql";
    
    private static final Integer WARM_DISCARD                 = 0;
    
    private static final Integer WARM_REPEAT                  = 1;
    
    private static final String  JDBC_CONNECTION;
    
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
    
    /**
     * Runs EXPLAIN to check the Postgresql plan for respective SQL queries.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    public void testExplain() throws Exception
    {
        explain(EXPLAIN_AP_FILE_NAME);
//        explain(EXPLAIN_NS_FILE_NAME);
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
        String explain = IoUtil.getResourceAsString(AbstractSetProcessableTestCaseNew.class, sql_file_name);
        
            Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection(JDBC_CONNECTION);
            
//            String explain_instance = explain.replaceAll("customers_000", "customers_x" + total_nations);
            Statement statement = connection.createStatement();
            statement.execute(explain);
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
            log.info("{} \n{}", new Object[] { sql_file_name, output });
            
            connection.close();
        
    }
    
    /**
     * Tests the apply-plan.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    public void testApply() throws Exception
    {
        run(NS_FILE_NAME, false);
    }
    
    /**
     * Tests the normalized-sets logical plan.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    public void testNS() throws Exception
    {
        run(NS_FILE_NAME, true);
        run(NS_FILE_NAME, false);
    }
    
    /**
     * Runs the query.
     * 
     * @param query_file_name
     *            the file name of the query.
     * @param rewrite
     *            whether to rewrite the apply-plan
     * @throws Exception
     *             if an exception occurs.
     */
    private static void run(String query_file_name, boolean rewrite) throws Exception
    {
        List<Object[]> results = Lists.newArrayList();
        double sum = 0.0;
        
        for (int i = 0; i < WARM_DISCARD; i++)
        {
            executeEndToEnd(query_file_name, null, rewrite);
        }
        
        for (int i = 0; i < WARM_REPEAT; i++)
        {
            double time = executeEndToEnd(query_file_name, null, rewrite);
            sum += time;
            results.add(new Object[] { query_file_name, time });
        }
        
        double average = sum / WARM_REPEAT;
        log.info("Results:");
        for (Object[] result : results)
        {
            log.info("{} {} ", result);
        }
        log.info("Average {} {}", new Object[] { query_file_name, average });
    }
    
    @Test
    public void testGenerate() throws Exception
    {
        String generate = IoUtil.getResourceAsString(AbstractSetProcessableTestCaseNew.class, GENERATE_FILE_NAME);
        
        for (Integer total_nations : new int[] { 5, 40, 400, 4000, 50000, 400000 })
        {
            for (Integer width : new int[] { 256 })
            {
                Class.forName("org.postgresql.Driver");
                Connection connection = DriverManager.getConnection(JDBC_CONNECTION);
                
                String generate_instance = generate.replaceAll("000", total_nations + "");
                generate_instance = generate_instance.replaceAll("XXX", width + "");
                log.info(generate_instance);
                Statement statement = connection.createStatement();
                statement.execute(generate_instance);
                statement.close();
                connection.close();
            }
        }
    }
}
