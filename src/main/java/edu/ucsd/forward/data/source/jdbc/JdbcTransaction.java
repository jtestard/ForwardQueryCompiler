/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.AbstractDataSourceTransaction;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.SchemaObject;
import edu.ucsd.forward.data.source.jdbc.model.Table;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryExecutionException;

/**
 * A JDBC transaction.
 * 
 * @author Kian Win
 * @author Yupeng
 * @author Michalis Petropoulos
 */
public class JdbcTransaction extends AbstractDataSourceTransaction
{
    private static final Logger log = Logger.getLogger(JdbcTransaction.class);
    
    private JdbcDataSource      m_data_source;
    
    private Connection          m_connection;
    
    /**
     * The temporary tables hosted by the connection.
     */
    private Map<String, Table>  m_temp_sql_tables;
    
    /**
     * Constructs a JDBC transaction.
     * 
     * @param data_source
     *            - the JDBC data source.
     * @throws QueryExecutionException
     *             if the transaction fails to begin.
     */
    public JdbcTransaction(JdbcDataSource data_source) throws QueryExecutionException
    {
        assert (data_source != null);
        m_data_source = data_source;
        
        try
        {
            assert (m_connection == null);
            m_connection = m_data_source.getConnectionPool().getConnection();
            assert (m_connection != null);
            assert (!m_connection.isClosed());
        }
        catch (SQLException ex)
        {
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.BEGIN, ex, this.getDataSource().getMetaData().getName());
        }
        catch (DataSourceException ex)
        {
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.BEGIN, ex, this.getDataSource().getMetaData().getName());
        }
        
        log.trace("JDBC Transaction started");
        
        m_temp_sql_tables = new HashMap<String, Table>();
        
        super.begin();
    }
    
    /**
     * Returns the underlying JDBC connection.
     * 
     * @return the JDBC connection.
     */
    public Connection getConnection()
    {
        return m_connection;
    }
    
    @Override
    public JdbcDataSource getDataSource()
    {
        return m_data_source;
    }
    
    /**
     * Gets the temporary SQL table by the given temporary schema name.
     * 
     * @param name
     *            the specific temporary schema object name.
     * @return the temporary SQL table corresponding to the temporary schema object.
     */
    protected Table getTemporarySqlTable(String name)
    {
        assert (m_temp_sql_tables.containsKey(name));
        
        return m_temp_sql_tables.get(name);
    }
    
    /**
     * Adds a temporary schema object to the connection.
     * 
     * @param schema_obj
     *            the temporary schema object to add.
     * @param table
     *            the SQL table corresponding to the schema object.
     * @exception QueryExecutionException
     *                if there is an existing temporary schema object with the same name.
     */
    public void addTemporarySchemaObject(SchemaObject schema_obj, Table table) throws QueryExecutionException
    {
        super.addTemporarySchemaObject(schema_obj);
        
        m_temp_sql_tables.put(schema_obj.getName(), table);
    }
    
    @Override
    public SchemaObject removeTemporarySchemaObject(String name) throws QueryExecutionException
    {
        assert (m_temp_sql_tables.containsKey(name));
        
        if(getState() != TransactionState.ABORTED)
        {
            // Drop the table
            JdbcTableBuilder.dropTable(m_temp_sql_tables.remove(name), this);
        }
        
        return super.removeTemporarySchemaObject(name);
    }
    
    @Override
    public void commit() throws QueryExecutionException
    {
        super.commit();
        
        try
        {
            assert (m_connection != null);
            assert (!m_connection.isClosed());
            
            m_connection.commit();
            m_connection.close();
            m_connection = null;
        }
        catch (SQLException ex)
        {
            this.rollback();
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.COMMIT, ex, this.getDataSource().getMetaData().getName());
        }
        
        log.trace("JDBC Transaction commited");
    }
    
    @Override
    public void rollback() throws QueryExecutionException
    {
        super.rollback();
        
        try
        {
            assert (m_connection != null);
            assert (!m_connection.isClosed());
            
            // Temporary schema objects are automatically dropped from the JDBC data source
            m_temp_sql_tables.clear();
            
            m_connection.rollback();
            m_connection.close();
            m_connection = null;
        }
        catch (SQLException ex)
        {
            super.rollback();
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.ROLLBACK, ex, this.getDataSource().getMetaData().getName());
        }
        
        log.trace("JDBC Transaction aborted");
    }
    
}
