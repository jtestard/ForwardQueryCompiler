/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.jdbc.model.SqlDialect;
import edu.ucsd.forward.exception.ExceptionMessages;

/**
 * A JDBC connection pool.
 * 
 * @author Kian Win
 * @author Yupeng
 */
public final class JdbcConnectionPool
{
    private static final Logger   log                                       = Logger.getLogger(JdbcConnectionPool.class);
    
    private static final String   MAX_POOL_SIZE                             = "maxPoolSize";
    
    private static final String   IDLE_CONNECTION_TEST_PERIOD               = "idleConnectionTestPeriod";
    
    private static final String   INITIAL_POOL_SIZE                         = "initialPoolSize";
    
    private static final String   MAX_IDLE_TIME                             = "maxIdleTime";
    
    private static final String   UNRETURNED_CONNECTION_TIMEOUT             = "unreturnedConnectionTimeout";
    
    private static final String   DEBUG_UNRETURNED_CONNECTION_STATCK_TRACES = "debugUnreturnedConnectionStackTraces";
    
    private ComboPooledDataSource m_pool;
    
    private JdbcDataSource        m_data_source;
    
    /**
     * Constructs the connection pool with the JDBC data source.
     * 
     * @param data_src
     *            the JDBC data source.
     * @param connection_url
     *            the connection URL of the JDBC data source.
     * @param properties
     *            the JDBC data source connection properties.
     * @throws DataSourceException
     *             if a connection cannot be obtained.
     */
    protected JdbcConnectionPool(JdbcDataSource data_src, String connection_url, Properties properties) throws DataSourceException
    {
        assert data_src != null;
        m_data_source = data_src;
        
        log.trace("Initializing JDBC connection pool for data source: " + m_data_source.getMetaData().getName());
        
        m_pool = new ComboPooledDataSource();
        m_pool.setJdbcUrl(connection_url);
        // This call only sets the properties of driver management.
        m_pool.setProperties(properties);
        
        // Explicitly call the setter for the pooled-connection properties.
        // Currently we only supported a limit number of properties.
        if (properties.containsKey(MAX_POOL_SIZE)) m_pool.setMaxPoolSize(Integer.parseInt((String) properties.get(MAX_POOL_SIZE)));
        if (properties.containsKey(IDLE_CONNECTION_TEST_PERIOD)) m_pool.setIdleConnectionTestPeriod(Integer.parseInt((String) properties.get(IDLE_CONNECTION_TEST_PERIOD)));
        if (properties.containsKey(INITIAL_POOL_SIZE)) m_pool.setInitialPoolSize(Integer.parseInt((String) properties.get(INITIAL_POOL_SIZE)));
        if (properties.containsKey(MAX_IDLE_TIME)) m_pool.setMaxIdleTime(Integer.parseInt((String) properties.get(MAX_IDLE_TIME)));
        if (properties.containsKey(UNRETURNED_CONNECTION_TIMEOUT)) m_pool.setUnreturnedConnectionTimeout(Integer.parseInt((String) properties.get(UNRETURNED_CONNECTION_TIMEOUT)));
        if (properties.containsKey(DEBUG_UNRETURNED_CONNECTION_STATCK_TRACES)) m_pool.setDebugUnreturnedConnectionStackTraces(Boolean.parseBoolean((String) properties.get(DEBUG_UNRETURNED_CONNECTION_STATCK_TRACES)));
        
        // Default retry attempts is 30, which causes long waits...
        m_pool.setAcquireRetryAttempts(3);
        
        setupDatabase();
    }
    
    /**
     * Runs SQL statements to setup the database.
     * 
     * @throws DataSourceException
     *             if a connection cannot be obtained.
     */
    protected void setupDatabase() throws DataSourceException
    {
        try
        {
            if (m_data_source.getSqlDialect() == SqlDialect.H2)
            {
                /*
                 * HACK:
                 * 
                 * All SQL statements are currently written hard-coded with the PostgreSQL temporary schema "pg_temp".
                 * 
                 * Create a schema of the same name in H2. This works however only when there is no concurrency, as there is only
                 * one global pg_temp schema, as opposed to Postgresql's behavior of a separate pg_temp schema for each connection.
                 */
                Statement s = getConnection().createStatement();
                s.executeUpdate("DROP SCHEMA pg_temp IF EXISTS");
                s.executeUpdate("CREATE SCHEMA pg_temp");
            }
        }
        catch (SQLException ex)
        {
            // Chain the exception
            throw new DataSourceException(ExceptionMessages.DataSource.JDBC_POOL_ERROR, ex, m_data_source.getMetaData().getName());
        }
        
    }
    
    /**
     * Gets the JDBC data source.
     * 
     * @return the JDBC data source.
     */
    public JdbcDataSource getDataSource()
    {
        return m_data_source;
    }
    
    /**
     * Retrieves a new connection from the pool.
     * 
     * @return a new connection from the pool.
     * @throws DataSourceException
     *             if a connection cannot be obtained.
     */
    public Connection getConnection() throws DataSourceException
    {
        try
        {
            log.trace("Connections: " + m_pool.getNumConnectionsAllUsers());
            log.trace("Busy conections: " + m_pool.getNumBusyConnectionsAllUsers());
            log.trace("Idle connections: " + m_pool.getNumIdleConnectionsAllUsers());
            log.trace("Orphaned connections: " + m_pool.getNumUnclosedOrphanedConnectionsAllUsers());
            
            Connection connection = m_pool.getConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            return connection;
        }
        catch (SQLException ex)
        {
            // Chain the exception
            throw new DataSourceException(ExceptionMessages.DataSource.JDBC_POOL_ERROR, ex, m_data_source.getMetaData().getName());
        }
    }
    
    /**
     * Close the connection pool.
     */
    public void close()
    {
        log.trace("Closing JDBC connection pool for data source: " + m_data_source.getMetaData().getName());
        
        // Close all resources of the connection pool
        m_pool.close();
    }
    
}
