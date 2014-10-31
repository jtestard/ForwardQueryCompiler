package edu.ucsd.forward.data.source.jdbc;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import com.mchange.v2.c3p0.C3P0ProxyConnection;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.index.IndexDeclaration;
import edu.ucsd.forward.data.index.IndexMethod;
import edu.ucsd.forward.data.index.KeyRange;
import edu.ucsd.forward.data.source.AbstractDataSource;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.DataSourceMetaData.StorageSystem;
import edu.ucsd.forward.data.source.DataSourceTransaction;
import edu.ucsd.forward.data.source.DataSourceTransaction.TransactionState;
import edu.ucsd.forward.data.source.SchemaObject;
import edu.ucsd.forward.data.source.SchemaObject.SchemaObjectScope;
import edu.ucsd.forward.data.source.jdbc.model.Index;
import edu.ucsd.forward.data.source.jdbc.model.Schema;
import edu.ucsd.forward.data.source.jdbc.model.SchemaHandle;
import edu.ucsd.forward.data.source.jdbc.model.SqlDialect;
import edu.ucsd.forward.data.source.jdbc.model.SqlIdentifierDictionary;
import edu.ucsd.forward.data.source.jdbc.model.Table;
import edu.ucsd.forward.data.source.jdbc.model.Table.TableScope;
import edu.ucsd.forward.data.source.jdbc.postgresql.JdbcIntrospectorPostgreSql;
import edu.ucsd.forward.data.source.jdbc.statement.CreateSchemaStatement;
import edu.ucsd.forward.data.source.jdbc.statement.DropSchemaStatement;
import edu.ucsd.forward.data.source.jdbc.statement.SimpleSelectStatement;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.TupleValue.AttributeValueEntry;
import edu.ucsd.forward.exception.ExceptionMessages;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryResult;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.SendPlanImpl;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.source_wrapper.sql.SqlSourceWrapper;
import edu.ucsd.forward.util.Timer;

/**
 * Represents a JDBC-accessible relational data source.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 */
public class JdbcDataSource extends AbstractDataSource
{
    private static final Logger     log      = Logger.getLogger(JdbcDataSource.class);
    
    /**
     * The properties to use when trying to connect to the JDBC data source, which must include at least a 'user' and a 'password'.
     */
    private Properties              m_connection_properties;
    
    private static final String     USERNAME = "user";
    
    private static final String     PASSWORD = "password";
    
    /**
     * A dictionary to store the correspondence between the original SQL identifier and the system-generated truncated version.
     */
    private SqlIdentifierDictionary m_identifier_dictionary;
    
    private SqlDialect              m_sql_dialect;
    
    private JdbcConnectionPool      m_connection_pool;
    
    private Map<String, Table>      m_sql_tables;
    
    private Map<String, Index>      m_sql_indices;
    
    /**
     * Constructs an instance of a JDBC data source.
     * 
     * @param name
     *            name of the JDBC data source.
     * @param storage_system
     *            the storage system of the JDBC data source.
     * @param connection_properties
     *            the connection properties of the JDBC data source.
     * @param overwrite
     *            indicates if the data source will be overwritten, or imported from an existing database.
     * @param exclusions
     *            what to exclude when importing an existing relational schema.
     * @throws DataSourceException
     *             when the right properties are not provided.
     * @throws QueryExecutionException
     *             if there is an error executing a query.
     */
    public JdbcDataSource(String name, StorageSystem storage_system, Properties connection_properties, boolean overwrite,
            List<JdbcExclusion> exclusions) throws DataSourceException, QueryExecutionException
    {
        super(new JdbcDataSourceMetaData(name, storage_system, connection_properties));
        
        String username = connection_properties.getProperty(USERNAME);
        String password = connection_properties.getProperty(PASSWORD);
        
        if (username == null || username.isEmpty() || password == null || password.isEmpty())
        {
            throw new DataSourceException(ExceptionMessages.DataSource.NO_JDBC_PROPERTIES, this.getMetaData().getName());
        }
        
        m_connection_properties = connection_properties;
        m_identifier_dictionary = new SqlIdentifierDictionary();
        m_sql_tables = new HashMap<String, Table>();
        m_sql_indices = new HashMap<String, Index>();
        initDialect();
        
        // Open the data source to either create or import a persistent relational schema
        this.open();
        
        if (overwrite)
        {
            // Create a new persistent relational schema
            createSchema();
        }
        else
        {
            importExistingSchema(exclusions);
        }
        
        // Close the data source after creating or importing a persistent relational schema
        this.close();
    }
    
    @Override
    public JdbcDataSourceMetaData getMetaData()
    {
        return (JdbcDataSourceMetaData) super.getMetaData();
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        assert (this.getState() == DataSourceState.CLOSED);
        
        super.open();
        
        // Create a connection pool
        try
        {
            m_connection_pool = new JdbcConnectionPool(this, getMetaData().getConnectionUrl(), m_connection_properties);
        }
        catch (DataSourceException e)
        {
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.DATA_SOURCE_ACCESS, e, this.getMetaData().getName());
        }
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        super.close();
        
        // Close the connection pool
        if (m_connection_pool != null)
        {
            m_connection_pool.close();
            m_connection_pool = null;
        }
    }
    
    @Override
    public JdbcTransaction beginTransaction() throws QueryExecutionException
    {
        assert (this.getState() == DataSourceState.OPEN);
        
        return new JdbcTransaction(this);
    }
    
    /**
     * Imports an existing schema.
     * 
     * @param exclusions
     *            what to exclude when importing an existing relational schema.
     * @throws DataSourceException
     *             if an error occurs while reading the metadata of the schema.
     * @throws QueryExecutionException
     *             if there is an error executing a query.
     */
    private void importExistingSchema(List<JdbcExclusion> exclusions) throws DataSourceException, QueryExecutionException
    {
        assert (this.getState() == DataSourceState.OPEN);
        
        switch (m_sql_dialect)
        {
            case POSTGRESQL:
                JdbcIntrospectorPostgreSql introspector = new JdbcIntrospectorPostgreSql(this);
                
                introspector.introspectSchemas(exclusions);
                
                introspector.introspectFunctions(exclusions);
                
                break;
            case H2:
                break;
        }
    }
    
    /**
     * Creates the schema for this data source.
     * 
     * @throws QueryExecutionException
     *             if a transaction fails.
     */
    private void createSchema() throws QueryExecutionException
    {
        assert (this.getState() == DataSourceState.OPEN);
        
        SchemaHandle handle = new SchemaHandle(new Schema(this.getMetaData().getSchemaName()));
        
        JdbcTransaction transaction = beginTransaction();
        
        // Drops the schema if it already exists (due to crashes etc.)
        DropSchemaStatement s1 = new DropSchemaStatement(handle);
        s1.setStrict(false);
        JdbcExecution.run(transaction, s1);
        
        // Creates the schema
        JdbcExecution.run(transaction, new CreateSchemaStatement(handle));
        
        transaction.commit();
    }
    
    /**
     * Returns the connection pool to this data source.
     * 
     * @return the connection pool to this data source.
     */
    public JdbcConnectionPool getConnectionPool()
    {
        return m_connection_pool;
    }
    
    /**
     * Returns a direct connection to this data source, that is, not through a connection pool.
     * 
     * @return a direct connection to this data source.
     * @throws DataSourceException
     *             when an exception is thrown.
     */
    public Connection getDirectConnection() throws DataSourceException
    {
        // Open a native connection, not one from the connection pool
        try
        {
            return DriverManager.getConnection(this.getMetaData().getConnectionUrl(), m_connection_properties);
        }
        catch (SQLException ex)
        {
            // Chain the exception
            throw new DataSourceException(ExceptionMessages.DataSource.JDBC_POOL_ERROR, ex, this.getMetaData().getName());
        }
    }
    
    @Override
    public SchemaObject createSchemaObject(String name, SchemaObjectScope scope, SchemaTree schema, Size estimate,
            DataSourceTransaction transaction) throws QueryExecutionException
    {
        assert (this.getState() == DataSourceState.OPEN);
        assert (transaction instanceof JdbcTransaction);
        assert (transaction.getState() == TransactionState.ACTIVE);
        
        SchemaObject schema_obj = null;
        switch (scope)
        {
            case TEMPORARY:
                // Create new schema object
                schema_obj = new SchemaObject(name, scope, schema, estimate);
                // Create new table
                Table table = JdbcTableBuilder.createTable(name, schema_obj, (JdbcTransaction) transaction);
                // Add the temporary object to the transaction
                ((JdbcTransaction) transaction).addTemporarySchemaObject(schema_obj, table);
                break;
            case PERSISTENT:
                // Create new schema object
                schema_obj = super.createSchemaObject(name, scope, schema, estimate, transaction);
                // Create new table
                m_sql_tables.put(name, JdbcTableBuilder.createTable(name, schema_obj, (JdbcTransaction) transaction));
                break;
            default:
                throw new UnsupportedOperationException();
        }
        
        return schema_obj;
    }
    
    /**
     * Imports an existing schema object with PERSISTENT scope to this data source.
     * 
     * @param schema_obj
     *            the schema object to import.
     * @throws QueryExecutionException
     *             if there is an existing schema object with the same name.
     */
    public void importSchemaObject(SchemaObject schema_obj) throws QueryExecutionException
    {
        assert (this.getState() == DataSourceState.OPEN);
        
        switch (m_sql_dialect)
        {
            case POSTGRESQL:
                JdbcTransaction transaction = beginTransaction();
                
                Table table = Table.create(TableScope.PERSISTENT, this.getMetaData().getSchemaName(), schema_obj.getName(),
                                           (CollectionType) schema_obj.getSchemaTree().getRootType());
                
                super.createSchemaObject(schema_obj.getName(), SchemaObjectScope.PERSISTENT, schema_obj.getSchemaTree(),
                                         schema_obj.getCardinalityEstimate(), transaction);
                m_sql_tables.put(schema_obj.getName(), table);
                
                transaction.commit();
                break;
            case H2:
                break;
        }
    }
    
    /**
     * Gets the SQL table by the given schema name.
     * 
     * @param name
     *            the specific schema object name.
     * @return the SQL table corresponding to the schema object
     */
    public Table getSqlTable(String name)
    {
        assert (m_sql_tables.containsKey(name));
        
        return m_sql_tables.get(name);
    }
    
    @Override
    public SchemaObject dropSchemaObject(String name, DataSourceTransaction transaction) throws QueryExecutionException
    {
        assert (transaction instanceof JdbcTransaction);
        
        // Check if the schema object is temporary
        if (transaction.hasTemporarySchemaObject(name))
        {
            return super.dropSchemaObject(name, transaction);
        }
        else
        {
            assert (m_sql_tables.containsKey(name));
            
            // Drop the table
            JdbcTableBuilder.dropTable(m_sql_tables.remove(name), (JdbcTransaction) transaction);
            
            return super.dropSchemaObject(name, transaction);
        }
    }
    
    @Override
    public void setDataObject(String name, DataTree data_tree, DataSourceTransaction transaction) throws QueryExecutionException
    {
        long time = System.currentTimeMillis();
        
        assert (this.getState() == DataSourceState.OPEN);
        assert (transaction instanceof JdbcTransaction);
        assert (transaction.getState() == TransactionState.ACTIVE);
        
        if (!this.hasSchemaObject(name, transaction))
        {
            Throwable cause = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_DATA_OBJ_NAME, name,
                                                      this.getMetaData().getName());
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.SET_DATA_OBJ_ERROR, cause, name, this.getMetaData().getName());
        }
        
        Table table = null;
        
        // Check if the schema object is temporary
        if (transaction.hasTemporarySchemaObject(name))
        {
            table = ((JdbcTransaction) transaction).getTemporarySqlTable(name);
        }
        else
        {
            table = m_sql_tables.get(name);
            
            // Clean the SQL table
            JdbcTableBuilder.truncate((JdbcTransaction) transaction, table);
        }
        
        if (data_tree != null)
        {
            // Populate the data in the data tree into SQL table
            JdbcTableBuilder.populate((JdbcTransaction) transaction, ((CollectionValue) data_tree.getRootValue()).getTuples(),
                                      table);
        }
        Timer.inc("Batch Insert Time", System.currentTimeMillis() - time);
        log.trace("Batch Insert Time :" + Timer.get("Batch Insert Time") + Timer.MS);
    }
    
    public void setDataObjectUsingCopy(String name, DataTree data_tree, DataSourceTransaction transaction)
            throws QueryExecutionException
    {
        long time = System.currentTimeMillis();
        
        assert (this.getState() == DataSourceState.OPEN);
        assert (transaction instanceof JdbcTransaction);
        assert (transaction.getState() == TransactionState.ACTIVE);
        
        if (!this.hasSchemaObject(name, transaction))
        {
            Throwable cause = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_DATA_OBJ_NAME, name,
                                                      this.getMetaData().getName());
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.SET_DATA_OBJ_ERROR, cause, name, this.getMetaData().getName());
        }
        
        Table table = null;
        
        // Check if the schema object is temporary
        if (transaction.hasTemporarySchemaObject(name))
        {
            table = ((JdbcTransaction) transaction).getTemporarySqlTable(name);
        }
        else
        {
            table = m_sql_tables.get(name);
            
            // Clean the SQL table
            JdbcTableBuilder.truncate((JdbcTransaction) transaction, table);
        }
        
        if (data_tree != null)
        {
            // Create CopyManager by using PGConnection
            CopyManager copy_manager = createCopyManager((JdbcTransaction) transaction);
            // Bulk import from reader into temporary table
            try
            {
                String data = writeDataObjectToString(((CollectionValue) data_tree.getRootValue())).toString();
                copy_manager.copyIn("copy " + table.getName() + " from stdin with null 'null'", new StringReader(data)); // FileReader
                JdbcTableBuilder.analyze(table, (JdbcTransaction) transaction);
            }
            catch (SQLException e)
            {
                throw new RuntimeException(e);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        Timer.inc("Bulk Copy Time", System.currentTimeMillis() - time);
        log.debug("Bulk Copy Time :" + Timer.get("Bulk Copy Time") + Timer.MS);
    }
    
    private StringBuilder writeDataObjectToString(CollectionValue collection)
    {
        StringBuilder sb = new StringBuilder();
        
        for (TupleValue tuple_value : collection.getTuples())
        {
            Iterator<AttributeValueEntry> it = tuple_value.iterator();
            while (it.hasNext())
            {
                AttributeValueEntry next = it.next();
                String escaped_value = escapeValueForCopy(next.getValue().toString());
                if (it.hasNext() == false)
                {
                    sb.append(escaped_value);
                }
                else
                {
                    sb.append(escaped_value + "\t");
                }
            }
            sb.append("\n");
        }
        return sb;
    }
    
    /**
     * Escapes a string value by escaping (\) backslash, (\n) new line, (\r) carriage return, and (\t) delimiter used by
     * writeDataObjectToString.
     * 
     * @param value
     *            the string value.
     * @return the escaped value.
     */
    private String escapeValueForCopy(String value)
    {
        String escaped = value.replace("\\", "\\\\");
        escaped = escaped.replace("\n", "\\n");
        escaped = escaped.replace("\r", "\\r");
        escaped = escaped.replace("\t", "\\t");
        return escaped;
    }
    
    private CopyManager createCopyManager(JdbcTransaction transaction)
    {
        assert (transaction != null);
        
        /*
         * Bulk import is not specified in JDBC. Instead, it is a vendor-specific method implemented by the Postgres JDBC driver,
         * and requires down-casting the Connection object to a PGConnection object.
         * 
         * The C3P0 connection pool, however, does not allow direct access to the raw Connection object. As a workaround, it exposes
         * a reflection API for invoking methods on the raw Connection object.
         * http://www.mchange.com/projects/c3p0/index.html#raw_connection_ops
         */
        
        // Get the method of the Postgres JDBC driver that returns a copy manager (which implements bulk import)
        Method copy_api_method;
        try
        {
            copy_api_method = PGConnection.class.getMethod("getCopyAPI");
        }
        catch (SecurityException e)
        {
            throw new RuntimeException(e);
        }
        catch (NoSuchMethodException e)
        {
            throw new RuntimeException(e);
        }
        
        // Get the C3P0 connection
        C3P0ProxyConnection c3p0_proxy_connection = (C3P0ProxyConnection) transaction.getConnection();
        
        // Use reflection to get the copy manager
        CopyManager copy_manager;
        try
        {
            copy_manager = (CopyManager) c3p0_proxy_connection.rawConnectionOperation(copy_api_method,
                                                                                      C3P0ProxyConnection.RAW_CONNECTION,
                                                                                      new Object[] {});
        }
        catch (IllegalArgumentException e)
        {
            throw new RuntimeException(e);
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
        catch (InvocationTargetException e)
        {
            throw new RuntimeException(e);
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e);
        }
        // finally
        // {
        // try
        // {
        // c3p0_proxy_connection.close();
        // }
        // catch(SQLException e)
        // {
        // throw new RuntimeException("Severe error! Cannot close C3P0ProxyConnection connection. " + e.getMessage());
        // }
        // }
        return copy_manager;
    }
    
    @Override
    public DataTree getDataObject(String name, DataSourceTransaction transaction) throws QueryExecutionException
    {
        assert (this.getState() == DataSourceState.OPEN);
        assert (transaction instanceof JdbcTransaction);
        assert (transaction.getState() == TransactionState.ACTIVE);
        JdbcTransaction jdbc_transaction = (JdbcTransaction) transaction;
        
        // A convenient way to get the view of the data tree stored in JDBC data source.
        SchemaObject schema_object = this.getSchemaObject(name, transaction);
        
        Table table = null;
        switch (schema_object.getScope())
        {
            case TEMPORARY:
                table = jdbc_transaction.getTemporarySqlTable(name);
                break;
            case PERSISTENT:
                table = getSqlTable(schema_object.getName());
        }
        
        SimpleSelectStatement statement = new SimpleSelectStatement(table);
        JdbcEagerResult result = JdbcExecution.run(jdbc_transaction, statement);
        CollectionValue collection = new CollectionValue();
        
        Binding binding = result.next();
        
        while (binding != null)
        {
            TupleValue tuple = new TupleValue();
            for (int i = 0; i < table.getColumns().size(); i++)
            {
                tuple.setAttribute(table.getColumns().get(i).getName(), binding.getValue(i).getValue());
            }
            collection.add(tuple);
            binding = result.next();
        }
        
        return new DataTree(collection);
    }
    
    @Override
    public void deleteDataObject(String name, DataSourceTransaction transaction) throws QueryExecutionException
    {
        assert (this.getState() == DataSourceState.OPEN);
        assert (transaction instanceof JdbcTransaction);
        assert (transaction.getState() == TransactionState.ACTIVE);
        JdbcTransaction jdbc_transaction = (JdbcTransaction) transaction;
        
        if (!this.hasSchemaObject(name, transaction))
        {
            Throwable cause = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_DATA_OBJ_NAME, name,
                                                      this.getMetaData().getName());
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.DELETE_DATA_OBJ_ERROR, cause, name, this.getMetaData().getName());
        }
        
        Table table = null;
        
        // Check if the schema object is temporary
        if (jdbc_transaction.hasTemporarySchemaObject(name))
        {
            table = jdbc_transaction.getTemporarySqlTable(name);
        }
        else
        {
            table = m_sql_tables.get(name);
        }
        
        // Clean the SQL table
        JdbcTableBuilder.truncate((JdbcTransaction) transaction, table);
    }
    
    @Override
    public QueryResult execute(PhysicalPlan physical_plan, DataSourceTransaction transaction) throws QueryExecutionException
    {
        assert (this.getState() == DataSourceState.OPEN);
        assert (transaction instanceof JdbcTransaction);
        assert (transaction.getState() == TransactionState.ACTIVE);
        
        // Make sure the top operator is not a send plan operator
        assert (!(physical_plan.getRootOperatorImpl() instanceof SendPlanImpl));
        
        // Translate the plan to query string
        long time = System.currentTimeMillis();
        
        SqlSourceWrapper sql_src_wrapper = new SqlSourceWrapper();
        String query_string = sql_src_wrapper.translatePlan(physical_plan, this);
        
        log.debug("\tSQL Generation Time: " + (System.currentTimeMillis() - time) + Timer.MS);
        
        // Executes the query string
        QueryResult result = sql_src_wrapper.execute(physical_plan, query_string, transaction);
        
        return result;
    }
    
    /**
     * Returns the dictionary for SQL identifiers.
     * 
     * @return the dictionary for SQL identifiers.
     */
    public SqlIdentifierDictionary getIdentifierDictionary()
    {
        return m_identifier_dictionary;
    }
    
    /**
     * Returns the SQL dialect.
     * 
     * @return the SQL dialect.
     */
    public SqlDialect getSqlDialect()
    {
        return m_sql_dialect;
    }
    
    /**
     * Initializes the SQL dialect.
     */
    protected void initDialect()
    {
        // TODO: Remove hardcoding when JDBC data source supports multiple dialects
        String database_driver = "postgresql";
        if (database_driver.equals("postgresql"))
        {
            m_sql_dialect = SqlDialect.POSTGRESQL;
        }
        else if (database_driver.equals("h2"))
        {
            m_sql_dialect = SqlDialect.H2;
        }
        else
        {
            assert (false);
        }
    }
    
    @Override
    public void createIndex(String data_obj_name, String name, SchemaPath collection_path, List<SchemaPath> key_paths,
            boolean unique, IndexMethod method, DataSourceTransaction transaction) throws QueryExecutionException
    {
        assert (this.getState() == DataSourceState.OPEN);
        assert (transaction instanceof JdbcTransaction);
        assert (transaction.getState() == TransactionState.ACTIVE);
        
        if (!m_sql_tables.containsKey(data_obj_name))
        {
            Throwable cause = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_COLLECTION_PATH, data_obj_name);
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.CREATE_INDEX_ERROR, cause, name, this.getMetaData().getName());
        }
        IndexDeclaration declaration = declareIndex(data_obj_name, name, collection_path, key_paths, unique, method);
        
        Index index = JdbcTableBuilder.createIndex(declaration, m_sql_tables.get(data_obj_name), (JdbcTransaction) transaction);
        m_sql_indices.put(name, index);
    }
    
    @Override
    public void deleteIndex(String data_obj_name, String name, SchemaPath collection_path, DataSourceTransaction transaction)
            throws QueryExecutionException
    {
        assert (this.getState() == DataSourceState.OPEN);
        assert (transaction instanceof JdbcTransaction);
        assert (transaction.getState() == TransactionState.ACTIVE);
        
        // Drop the Index in schema tree
        dropIndexDeclaration(data_obj_name, name, collection_path);
        
        Index index = m_sql_indices.get(name);
        JdbcTableBuilder.dropIndex(index, (JdbcTransaction) transaction);
        m_sql_indices.remove(name);
    }
    
    @Override
    public List<TupleValue> getFromIndex(String data_obj_name, SchemaPath collection_path, String name, List<KeyRange> ranges,
            DataSourceTransaction transaction) throws QueryExecutionException
    {
        throw new UnsupportedOperationException();
    }
    
}
