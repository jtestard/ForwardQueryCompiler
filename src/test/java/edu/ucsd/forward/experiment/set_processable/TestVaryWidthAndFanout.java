/**
 * 
 */
package edu.ucsd.forward.experiment.set_processable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;
import org.testng.v6.Lists;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.CheckedException;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.rewrite.ApplyPlanRemover;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Tests varying the tuple width and fanout.
 * 
 * @author Yupeng Fu
 * 
 */
@Test(groups = edu.ucsd.forward.test.AbstractTestCase.PERFORMANCE)
public class TestVaryWidthAndFanout extends AbstractSetProcessableTestCase
{
    private static final Logger        log                        = Logger.getLogger(TestVaryWidthAndFanout.class);
    
    private static final String        GENERATE_FILE_NAME         = "TestVaryWidthAndSelectivity-Generate.sql";
    
    private static final String        SELECTED_NATIONS_FILE_NAME = "selected-nations-10.xml";
    
    private static final String        QUERY_FILE_NAME            = "TestVaryWidthAndSelectivity-Query.xml";
    
    private static final String        DS_FILE_NAME               = "TestVaryWidthAndSelectivity-DS.xml";
    
    private static final String        DS_SQL_FILE_NAME           = "TestVaryWidthAndSelectivity-DS.sql";
    
    private static final List<Integer> TOTAL_NATIONS              = new ArrayList<Integer>();
    
    private static final List<Integer> TOTAL_WIDTHS               = new ArrayList<Integer>();
    
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
    
    static
    {
        TOTAL_WIDTHS.add(4);
        TOTAL_WIDTHS.add(16);
        TOTAL_WIDTHS.add(64);
        TOTAL_WIDTHS.add(256);
        TOTAL_WIDTHS.add(1024);
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
    
    private static final Integer       DISCARD                    = 1;
    
    private static final Integer       REPEAT                     = 2;
    
    /**
     * Generates the corresponding <code>nations_x000_wXXX</code>.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    public void testGenerate() throws Exception
    {
        generate();
    }
    
    /**
     * Runs EXPLAIN to check the Postgresql plan for respective SQL queries.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    public void testExplainDS() throws Exception
    {
        String explain = IoUtil.getResourceAsString(AbstractSetProcessableTestCase.class, DS_SQL_FILE_NAME);
        
        for (Integer total_nations : TOTAL_NATIONS)
        {
            for (Integer width : TOTAL_WIDTHS)
            {
                Class.forName("org.postgresql.Driver");
                Connection connection = DriverManager.getConnection(JDBC_CONNECTION);
                
                String explain_instance = explain.replaceAll("public.nation", "public.nations_x" + total_nations + "_w" + width);
                explain_instance = explain_instance.replaceAll("public.customers", "public.customers_x" + total_nations);
                explain_instance = explain_instance.replaceFirst("select ", "explain select ");
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
                log.info("{} {}\n{}", new Object[] { DS_SQL_FILE_NAME, total_nations, output });
                
                connection.close();
            }
        }
    }
    
    public void testDS() throws Exception
    {
        // parseTestCase(this.getClass(), DS_FILE_NAME);
        //
        // String query_expr = getQueryExpression(0);
        // log.info(this.explainQuery(query_expr));
        run(DS_FILE_NAME);
    }
    
    public void testDSInSql() throws Exception
    {
        String sql = IoUtil.getResourceAsString(AbstractSetProcessableTestCase.class, DS_SQL_FILE_NAME);
        List<Object[]> results = Lists.newArrayList();
        for (Integer total_nations : TOTAL_NATIONS)
        {
            for (Integer width : TOTAL_WIDTHS)
            {
                double sum = 0.0;
                
                for (int i = 0; i < DISCARD; i++)
                {
                    Class.forName("org.postgresql.Driver");
                    Connection connection = DriverManager.getConnection(JDBC_CONNECTION);
                    
                    String sql_instance = sql.replaceAll("public.nation", "public.nations_x" + total_nations + "_w" + width);
                    sql_instance = sql_instance.replaceAll("public.customers", "public.customers_x" + total_nations);
                    log.info(sql_instance);
                    Statement statement = connection.createStatement();
                    statement.execute(sql_instance);
                    statement.close();
                    connection.close();
                }
                
                for (int i = 0; i < REPEAT; i++)
                {
                    Class.forName("org.postgresql.Driver");
                    Connection connection = DriverManager.getConnection(JDBC_CONNECTION);
                    
                    String sql_instance = sql.replaceAll("public.nation", "public.nations_x" + total_nations + "_w" + width);
                    sql_instance = sql_instance.replaceAll("public.customers", "public.customers_x" + total_nations);
                    log.info(sql_instance);
                    // Start timing
                    long start = System.currentTimeMillis();
                    Statement statement = connection.createStatement();
                    statement.execute(sql_instance);
                    statement.close();
                    connection.close();
                    // End timing
                    long end = (System.currentTimeMillis() - start);
                    sum += end;
                }
                
                double average = sum / REPEAT;
                results.add(new Object[] { QUERY_FILE_NAME, total_nations, width, average });
                log.info("{} {} {} {} ", new Object[] { QUERY_FILE_NAME, total_nations, width, average });
            }
        }
        log.info("Results:");
        for (Object[] result : results)
        {
            log.info("{} {} {} {} ", result);
        }
    }
    
    public void testNS() throws Exception
    {
        List<Object[]> results = Lists.newArrayList();
        for (Integer total_nations : TOTAL_NATIONS)
        {
            for (Integer width : TOTAL_WIDTHS)
            {
                double sum = 0.0;
                
                for (int i = 0; i < DISCARD; i++)
                {
                    // Parse the logical plan and data sources, and open the UAS
                    parse(QUERY_FILE_NAME, SELECTED_NATIONS_FILE_NAME, null);
                    String query_string = getQueryExpression(0);
                    
                    query_string = query_string.replaceAll("public.nation", "public.nations_x" + total_nations + "_w" + width);
                    query_string = query_string.replaceAll("public.customers", "public.customers_x" + total_nations);
                    executeQuery(query_string, null, true);
                }
                
                for (int i = 0; i < REPEAT; i++)
                {
                    // Parse the logical plan and data sources, and open the UAS
                    parse(QUERY_FILE_NAME, SELECTED_NATIONS_FILE_NAME, null);
                    String query_string = getQueryExpression(0);
                    
                    query_string = query_string.replaceAll("public.nation", "public.nations_x" + total_nations + "_w" + width);
                    query_string = query_string.replaceAll("public.customers", "public.customers_x" + total_nations);
                    sum += executeQuery(query_string, null, true);
                }
                
                double average = sum / REPEAT;
                results.add(new Object[] { QUERY_FILE_NAME, total_nations, width, average });
                log.info("{} {} {} {} ", new Object[] { QUERY_FILE_NAME, total_nations, width, average });
            }
        }
        log.info("Results:");
        for (Object[] result : results)
        {
            log.info("{} {} {} {} ", result);
        }
    }
    
    private double executeQuery(String query, Map<String, String> rename_variables_map, boolean rewrite)
            throws CheckedException, SuspensionException
    {
        // Obtain the UAS and physical plan
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        List<AstTree> ast_trees = qp.parseQuery(query, new LocationImpl());
        assert (ast_trees.size() == 1);
        
        LogicalPlan actual = qp.translate(Collections.singletonList(ast_trees.get(0)), uas).get(0);
        if (rewrite)
        {
            actual = ApplyPlanRemover.create(true).rewrite(actual);
        }
        LogicalPlan rewritten = qp.rewriteSourceAgnostic(Collections.singletonList(actual), uas).get(0);
        LogicalPlan distributed = qp.distribute(Collections.singletonList(rewritten), uas).get(0);
        PhysicalPlan physical = qp.generate(Collections.singletonList(distributed), uas).get(0);
        
        // Start timing
        long start = System.currentTimeMillis();
        
        // Execute the physical plan
        Value query_result = QueryProcessorFactory.getInstance().createEagerQueryResult(physical, uas).getValue();
        
        log.info(query_result.toString());
        // End timing
        long end = (System.currentTimeMillis() - start);
        
        // Clean up
        qp.cleanup(true);
        uas.close();
        qp = null;
        
        return end;
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
            for (Integer width : TOTAL_WIDTHS)
            {
                Map<String, String> rename_variables = new LinkedHashMap<String, String>();
                rename_variables.put("nation", "nations_x" + total_nations + "_w" + width);
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
        }
        log.info("Results:");
        for (Object[] result : results)
        {
            log.info("{} {} {}", result);
        }
    }
    
    /**
     * Generates the corresponding <code>nations_x000_wXXX</code>for each TOTAL_NATION.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    private static void generate() throws Exception
    {
        String generate = IoUtil.getResourceAsString(AbstractSetProcessableTestCase.class, GENERATE_FILE_NAME);
        
        for (Integer total_nations : TOTAL_NATIONS)
        {
            for (Integer width : TOTAL_WIDTHS)
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
