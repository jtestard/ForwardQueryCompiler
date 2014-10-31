/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.ucsd.app2you.util.config.Config;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceTransaction.TransactionState;
import edu.ucsd.forward.data.source.jdbc.model.SqlDialect;
import edu.ucsd.forward.data.source.jdbc.model.SqlIdentifierDictionary;
import edu.ucsd.forward.data.source.jdbc.model.SqlTypeConverter;
import edu.ucsd.forward.data.source.jdbc.statement.JdbcStatement;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.dml.AbstractDmlOperator;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.util.Timer;

/**
 * An execution of a JDBC statement.
 * 
 * @author Kian Win
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public class JdbcExecution
{
    private static final Logger log = Logger.getLogger(JdbcExecution.class);
    
    private JdbcTransaction     m_transaction;
    
    private JdbcStatement       m_statement;
    
    private PreparedStatement   m_prepared_statement;
    
    private String              m_sql_string;
    
    private State               m_state;
    
    /**
     * Constructs a JDBC execution.
     * 
     * @param transaction
     *            - the JDBC transaction.
     * @param statement
     *            - the JDBC statement.
     * @throws QueryExecutionException
     *             if an exception is thrown during query execution.
     */
    public JdbcExecution(JdbcTransaction transaction, JdbcStatement statement) throws QueryExecutionException
    {
        assert (transaction != null);
        assert (statement != null);
        
        m_transaction = transaction;
        m_statement = statement;
        m_state = State.CLOSED;
        
        this.init();
    }
    
    /**
     * An enumeration of the execution states.
     */
    public enum State
    {
        OPEN, CLOSED;
    }
    
    /**
     * Initializes the execution. This should only be called once.
     * 
     * @throws QueryExecutionException
     *             if an exception is thrown during query execution.
     */
    private void init() throws QueryExecutionException
    {
        assert m_state == State.CLOSED;
        assert (m_prepared_statement == null);
        assert (m_transaction.getState() == TransactionState.ACTIVE);
        
        m_state = State.OPEN;
        
        SqlDialect sql_dialect = m_transaction.getDataSource().getSqlDialect();
        SqlIdentifierDictionary dictionary = m_transaction.getDataSource().getIdentifierDictionary();
        
        m_sql_string = m_statement.toSql(sql_dialect, dictionary);
        
        // Prepare the JDBC statement
        try
        {
            // log.debug("\n{}", m_sql_string);
            // System.out.println(m_sql_string);
            //
            m_prepared_statement = m_transaction.getConnection().prepareStatement(m_sql_string);
        }
        catch (SQLException ex)
        {
            throw new QueryExecutionException(QueryExecution.JDBC_STMT_EXEC_ERROR, ex,
                                              m_transaction.getDataSource().getMetaData().getName(), m_sql_string);
        }
        
        String query_plan = "\nQuery:\n" + m_sql_string;
        
        // Display the plan of the query only if it is enabled
        if (Config.isExplainJdbcQueries())
        {
            try
            {
                // Currently only display SELECT clause
                if (m_sql_string.startsWith("SELECT ALL"))
                {
                    Statement statement = m_transaction.getConnection().createStatement();
                    statement.execute("EXPLAIN " + m_sql_string);
                    ResultSet result = statement.getResultSet();
                    query_plan += "\nQuery Plan:\n";
                    while (result.next())
                    {
                        query_plan += result.getString(1) + "\n";
                    }
                }
            }
            catch (SQLException ex)
            {
                throw new QueryExecutionException(QueryExecution.JDBC_STMT_EXEC_ERROR, ex,
                                                  m_transaction.getDataSource().getMetaData().getName(), m_sql_string);
            }
        }
        log.debug(query_plan);
    }
    
    /**
     * Ends the execution. This should only be called once to clean up resources. There is no guarantee on whether returned results
     * can still be used after an execution has been ended.
     * 
     * @throws QueryExecutionException
     *             if an exception is thrown during query execution.
     * 
     */
    public void end() throws QueryExecutionException
    {
        assert m_state == State.OPEN;
        assert (m_prepared_statement != null);
        m_state = State.CLOSED;
        try
        {
            m_prepared_statement.close();
        }
        catch (SQLException ex)
        {
            throw new QueryExecutionException(QueryExecution.JDBC_STMT_EXEC_ERROR, ex,
                                              m_transaction.getDataSource().getMetaData().getName(), m_sql_string);
        }
    }
    
    /**
     * Adds a set of parameters to the prepared statement's batch of commands. The given scalar values are about to instantiate the
     * statement's parameters according to the ordinal position.
     * 
     * @param arguments
     *            - the value of the arguments.
     * @throws QueryExecutionException
     *             if an exception is thrown during query execution.
     */
    public void addBatch(Value... arguments) throws QueryExecutionException
    {
        try
        {
            for (int position = 0; position < arguments.length; position++)
            {
                // log.trace("Parameter has value {} at position {}", new Object[] { arguments[position], position });
                SqlTypeConverter.toSql(m_prepared_statement, position + 1, arguments[position]);
            }
            
            // Add to batch
            m_prepared_statement.addBatch();
        }
        catch (SQLException ex)
        {
            throw new QueryExecutionException(QueryExecution.JDBC_STMT_EXEC_ERROR, ex,
                                              m_transaction.getDataSource().getMetaData().getName(), m_sql_string);
        }
    }
    
    /**
     * Submits a batch of commands to the database for execution and if all commands execute successfully, returns a list of
     * bindings, each of which has an integer value representing the update counts. The returned bindings are ordered to correspond
     * to the commands in the batch, which are ordered according to the order in which they were added to the batch. The integer in
     * each binding may be one of the following:
     * <ul>
     * <li>A number greater than or equal to zero -- indicates that the command was processed successfully and is an update count
     * giving the number of rows in the database that were affected by the command's execution</li>
     * 
     * <li>A value of -2, which is a JDBC constant <code>SUCCESS_NO_INFO</code>, indicating that the command was processed
     * successfully but that the number of rows affected is unknown If one of the commands in a batch update fails to execute
     * properly, this method throws a BatchUpdateException, and a JDBC driver may or may not continue to process the remaining
     * commands in the batch.
     * <li>
     * 
     * <li>A value of -3, which is a JDBC constant <code>EXECUTE_FAILED</code>, indicating that the command failed to execute
     * successfully and occurs only if a driver continues to process commands after a command fails</li>
     * </ul>
     * 
     * @return a list of bindings, each of which containing one update counts for each command in the batch.
     * @throws QueryExecutionException
     *             if an exception is thrown during query execution.
     */
    public JdbcResult executeBatch() throws QueryExecutionException
    {
        try
        {
            List<Binding> bindings = new ArrayList<Binding>();
            int[] counts = m_prepared_statement.executeBatch();
            for (int i = 0; i < counts.length; i++)
            {
                int update_count = counts[i];
                
                Binding binding = new Binding();
                IntegerValue count_value = new IntegerValue(update_count);
                TupleValue tup_value = new TupleValue();
                tup_value.setAttribute(AbstractDmlOperator.COUNT_ATTR, count_value);
                binding.addValue(new BindingValue(tup_value, true));
                
                bindings.add(binding);
            }
            
            return new JdbcEagerResult(bindings);
        }
        catch (SQLException ex)
        {
            SQLException root = ex;
            String cause = "";
            while (ex != null)
            {
                cause += ex.getMessage(); // Log the exception
                // Get cause if present
                Throwable t = ex.getCause();
                while (t != null)
                {
                    cause += "Cause:" + t;
                    t = t.getCause();
                }
                // procees to the next exception
                ex = ex.getNextException();
            }
            throw new QueryExecutionException(QueryExecution.JDBC_STMT_EXEC_ERROR,
                                              m_transaction.getDataSource().getMetaData().getName(), m_sql_string, cause);
        }
    }
    
    /**
     * Runs the execution. The given scalar values will instantiate the statement's parameters according to their ordinal position.
     * Note that each argument value has to have an associated type. If the statement produces no results, returns a binding
     * containing an value of either the row count for INSERT, UPDATE or DELETE statements, or 0 for JDBC statements that return
     * nothing.
     * 
     * @param arguments
     *            - the value of the arguments.
     * @return the JDBC result
     * @throws QueryExecutionException
     *             if an exception is thrown during query execution.
     */
    public JdbcResult run(Value... arguments) throws QueryExecutionException
    {
        try
        {
            long time = System.currentTimeMillis();
            
            for (int position = 0; position < arguments.length; position++)
            {
                log.debug("Parameter has value {} at position {}", new Object[] { arguments[position], position });
                SqlTypeConverter.toSql(m_prepared_statement, position + 1, arguments[position]);
            }
            
            // m_prepared_statement.setFetchSize(10);
            
            // Run the JDBC prepared statement
            boolean has_results = m_prepared_statement.execute();
            
            // log.debug(m_sql_string.toString());
            
            JdbcResult result;
            
            if (has_results)
            {
                // Limitation: assume that the prepared statement returns only one ResultSet
                result = new JdbcLazyResult(m_prepared_statement.getResultSet());
            }
            else
            {
                // For API uniformity, return the update count as a result too
                int update_count = m_prepared_statement.getUpdateCount();
                
                Binding binding = new Binding();
                binding.addValue(new BindingValue(new IntegerValue(update_count), true));
                
                result = new JdbcEagerResult(Arrays.asList(binding));
            }
            
            Timer.inc("Jdbc Call Execution", System.currentTimeMillis() - time);
            log.debug("Jdbc Call Execution : " + Timer.get("Jdbc Call Execution") + Timer.MS);
            // System.out.println("Jdbc Call Execution : " + Timer.get("Jdbc Call Execution") + Timer.MS);
            Timer.reset("Jdbc Call Execution");
            
            return result;
        }
        catch (SQLException ex)
        {
            SQLException root = ex;
            String cause = "";
            while (ex != null)
            {
                cause += ex.getMessage(); // Log the exception
                // Get cause if present
                Throwable t = ex.getCause();
                while (t != null)
                {
                    cause += "Cause:" + t;
                    t = t.getCause();
                }
                // procees to the next exception
                ex = ex.getNextException();
            }
            throw new QueryExecutionException(QueryExecution.JDBC_STMT_EXEC_ERROR, root,
                                              m_transaction.getDataSource().getMetaData().getName(), m_sql_string, cause);
        }
    }
    
    /**
     * Utility method that runs a JDBC statement in isolation, using the given transaction, and returns an eager result. The given
     * scalar values will instantiate the statement's parameters according to their ordinal position. Note that each argument value
     * has to have an associated type. If the statement produces no results, returns a binding containing an value of either the row
     * count for INSERT, UPDATE or DELETE statements, or 0 for SQL statements that return nothing.
     * 
     * @param transaction
     *            - the transaction.
     * @param statement
     *            - the JDBC statement.
     * @param arguments
     *            - the argument values.
     * 
     * @return an eager JDBC result object that contains the data produced by the given query; never null.
     * @throws QueryExecutionException
     *             if an exception is thrown during query execution.
     */
    public static JdbcEagerResult run(JdbcTransaction transaction, JdbcStatement statement, Value... arguments)
            throws QueryExecutionException
    {
        assert (transaction != null);
        
        JdbcExecution execution = new JdbcExecution(transaction, statement);
        JdbcResult lazy_result = execution.run(arguments);
        JdbcEagerResult eager_result = new JdbcEagerResult(lazy_result);
        execution.end();
        
        return eager_result;
    }
    
    /**
     * Gets the state of the JDBC execution.
     * 
     * @return the state of the JDBC execution
     */
    public State getState()
    {
        return m_state;
    }
    
    /**
     * Find a value based on a hierarchical naming of nested tuples.
     * 
     * @param parts
     *            - the hierarchical parts.
     * @param tuple
     *            - the tuple to start finding.
     * @return the value.
     */
    protected static Value findValue(List<String> parts, TupleValue tuple)
    {
        assert parts != null;
        assert !parts.isEmpty();
        assert tuple != null;
        
        if (parts.size() == 1)
        {
            // Base case - the attribute is a scalar value
            return tuple.getAttribute(parts.get(0));
        }
        else
        {
            // Recurse - the attribute is a tuple
            TupleValue child_tuple = (TupleValue) tuple.getAttribute(parts.get(0));
            return findValue(parts.subList(1, parts.size()), child_tuple);
        }
    }
    
    /**
     * Returns the SQL string mangled appropriately for the given SQL dialect. The mangling is through regexes. There is a
     * infinitesimal chance that the regexes will replace strings other than those intended, but this is acceptable until we
     * implement a real statement parser.
     * 
     * @param string
     *            - the string.
     * @param sql_dialect
     *            - the SQL dialect.
     * @return the SQL string for the dialect.
     */
    protected String getSqlString(String string, SqlDialect sql_dialect)
    {
        // Use a regex to replace all SQL identifiers that may be longer than Postgresql's maximum identifier limit
        String s = string;
        if (sql_dialect == SqlDialect.POSTGRESQL)
        {
            // s = m_transaction.getDataSource().getIdentifierDictionary().replacePrettyStrings(s);
            // Now the replacement is pop up to the higher level during producing the SQL string
        }
        else
        {
            assert (sql_dialect == SqlDialect.H2);
            // Do nothing
        }
        
        // PostgreSQL uses the non-standard RANDOM() instead of the standard RAND()
        if (sql_dialect == SqlDialect.POSTGRESQL)
        {
            // Do nothing
            assert (true);
        }
        else
        {
            assert (sql_dialect == SqlDialect.H2);
            
            // Match case-insensitive occurrences of "RANDOM()"
            s = s.replaceAll("(?i)RANDOM\\(\\)", "RAND()");
        }
        
        return s;
    }
    
    @Override
    public String toString()
    {
        return m_sql_string;
    }
}
