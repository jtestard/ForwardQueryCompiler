/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.postgresql;

import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import edu.ucsd.app2you.util.ReflectionUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceTransaction;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.source.SchemaObject.SchemaObjectScope;
import edu.ucsd.forward.data.source.jdbc.JdbcConnectionPool;
import edu.ucsd.forward.data.source.jdbc.JdbcDataSource;
import edu.ucsd.forward.data.source.jdbc.JdbcTransaction;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.fpl.AbstractFplTestCase;
import edu.ucsd.forward.fpl.FplInterpreter;
import edu.ucsd.forward.fpl.FplInterpreterFactory;
import edu.ucsd.forward.fpl.ast.ActionDefinition;
import edu.ucsd.forward.fpl.ast.Definition;
import edu.ucsd.forward.fpl.ast.FunctionDefinition;
import edu.ucsd.forward.query.EagerQueryResult;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.logical.CardinalityEstimate;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * Tests JDBC connection is cleaned properly (resources and connection
 * released).
 * 
 * @author Erick Zamora
 * 
 */
@Test
public class TestJdbcConnection extends AbstractFplTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestJdbcConnection.class);
    
    /**
     * Tests whether schema tables were cleaned after a transaction is manually aborted.
     * 
     * @throws Exception
     *      if an error occurs.
     */
    public void testTransactionCommitted() throws Exception
    {
        String xml_test_case = "TestJdbcConnection-testTransactionCommitted.xml";
        
        parseTestCase(this.getClass(), xml_test_case);
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        JdbcDataSource jdbc_data_source = (JdbcDataSource) uas.getDataSource("jdbc");
        
        SchemaTree schema = null;
        {
            CollectionType collection = new CollectionType();
              
            TupleType tuple = new TupleType();
            tuple.setAttribute("_v1", new StringType());
            collection.setTupleType(tuple);
              
            schema = new SchemaTree(collection);
        }
        
        try
        {
            DataSourceTransaction transaction = jdbc_data_source.beginTransaction();
            jdbc_data_source.createSchemaObject("_v2", SchemaObjectScope.TEMPORARY, 
                                                schema, CardinalityEstimate.Size.ONE, transaction);
            transaction.commit();
            
            DataSourceTransaction transaction_2 = jdbc_data_source.beginTransaction();
            jdbc_data_source.createSchemaObject("_v2", SchemaObjectScope.TEMPORARY, schema, 
                                                CardinalityEstimate.Size.ONE, transaction_2);
            transaction_2.commit();
        }
        catch(QueryExecutionException e)
        {
            assert(false);
        }
    }
    
    /**
     * Tests whether schema tables were cleaned after a transaction is commited.
     * 
     * @throws Exception
     *      if an error occurs.
     */
    public void testTransactionRollback() throws Exception
    {
        String xml_test_case = "TestJdbcConnection-testTransactionRollback.xml";
        
        parseTestCase(this.getClass(), xml_test_case);
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        JdbcDataSource jdbc_data_source = (JdbcDataSource) uas.getDataSource("jdbc");
        
        SchemaTree schema = null;
        {
            CollectionType collection = new CollectionType();
              
            TupleType tuple = new TupleType();
            tuple.setAttribute("_v1", new StringType());
            collection.setTupleType(tuple);
              
            schema = new SchemaTree(collection);
        }
        
        try
        {
            JdbcTransaction transaction = jdbc_data_source.beginTransaction();
            
            // 'jdbc.executed_query_patterns gets converted to public.executed_query_patterns'
            PreparedStatement executed_queries = transaction.getConnection()
                    .prepareStatement("select * from public.executed_query_patterns");
            
            jdbc_data_source.createSchemaObject("_v2", SchemaObjectScope.TEMPORARY, 
                                                schema, CardinalityEstimate.Size.ONE, transaction);
            
            // Make sure there are rows in executed queries table
            assert(executed_queries.executeQuery().next());
            
            // Delete all rows from table
            transaction.getConnection().prepareStatement("delete from public.executed_query_patterns").execute();
            
            // Make sure no rows exist
            assert(!executed_queries.executeQuery().next());
            
            transaction.rollback();
            
            /* Test that the rows in executed_query_patterns table were not deleted, but the temporary
             * schema object was */
            {
                transaction = jdbc_data_source.beginTransaction();
                
                // 'jdbc.executed_query_patterns gets converted to public.executed_query_patterns'
                executed_queries = transaction.getConnection()
                        .prepareStatement("select * from public.executed_query_patterns");
                
                // Test that the temporary schema was dropped with rollback by reinserting it here
                jdbc_data_source.createSchemaObject("_v2", SchemaObjectScope.TEMPORARY, 
                                                    schema, CardinalityEstimate.Size.ONE, transaction);
                
                // Make sure there are rows in executed queries table
                assert(executed_queries.executeQuery().next());
            }
        }
        catch(QueryExecutionException e)
        {
            assert(false);
        }
    }
    
    /**
     * Tests whether an internally aborted jdbc transaction releases resources properly and returns the connection back to the
     * connection pool.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testTransactionAborted() throws Exception
    {
        String xml_test_case = "TestJdbcConnection-testTransactionAborted.xml";
        
        EagerQueryResult eager_query_result = getEagerQueryResult(xml_test_case);
        
        try
        {
            // Runs the action which should throw a PSqlException
            eager_query_result.getValue();
        }
        catch (QueryExecutionException e)
        {
            // Get the jdbc connection
            JdbcConnectionPool jdbc_connection_pool = 
                ((JdbcDataSource) getUnifiedApplicationState().getDataSource("jdbc")).getConnectionPool();
            
            // Get the underlying pool manager using reflection
            ComboPooledDataSource pool_manager = 
                    (ComboPooledDataSource) ReflectionUtil.getMemberVariable(jdbc_connection_pool, "m_pool");
            
            assert pool_manager.getNumUnclosedOrphanedConnectionsAllUsers() == 0 
                : "JdbcConnection not properly returned to connection pool.";
        }
    }
    /**
     * 
     * @param xml_test_case
     * @return
     * @throws Exception
     */
    public EagerQueryResult getEagerQueryResult(String xml_test_case) throws Exception
    {
        Location location = new LocationImpl(xml_test_case);
        
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), xml_test_case);
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        // Parse and compile actions
        if(!getActionExpressions().isEmpty())
        {
            String action_str = getActionExpression(0);
            FplInterpreter interpreter = FplInterpreterFactory.getInstance();
            interpreter.setUnifiedApplicationState(uas);
            List<Definition> definitions = interpreter.parseFplCode(action_str, location);
            for (Definition definition : definitions)
            {
                if (definition instanceof ActionDefinition) continue;
                interpreter.registerFplFunction((FunctionDefinition) definition);
            }
            for (Definition definition : definitions)
            {
                if (definition instanceof ActionDefinition) continue;
                interpreter.compileFplFunction((FunctionDefinition) definition);
            }
        }
        
        // Gets the query expression, which is the entry point
        String query_str = getQueryExpression(0);
        QueryProcessor processor = QueryProcessorFactory.getInstance();
        AstTree ast_tree = processor.parseQuery(query_str, location).get(0);
        PhysicalPlan physical_plan = processor.compile(Collections.singletonList(ast_tree), uas).get(0);
        
        return processor.createEagerQueryResult(physical_plan, uas);
    }
}
