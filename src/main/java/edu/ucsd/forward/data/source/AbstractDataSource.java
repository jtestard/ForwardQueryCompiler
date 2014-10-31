package edu.ucsd.forward.data.source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.index.IndexDeclaration;
import edu.ucsd.forward.data.index.IndexMethod;
import edu.ucsd.forward.data.index.KeyRange;
import edu.ucsd.forward.data.source.DataSourceTransaction.TransactionState;
import edu.ucsd.forward.data.source.SchemaObject.SchemaObjectScope;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.exception.ExceptionMessages;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryResult;
import edu.ucsd.forward.query.function.FunctionRegistryException;
import edu.ucsd.forward.query.function.external.ExternalFunction;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * Abstract class for implementing data sources.
 * 
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractDataSource implements DataSource
{
    private static final Logger           log         = Logger.getLogger(AbstractDataSource.class);
    
    /**
     * The data source meta data.
     */
    private DataSourceMetaData            m_meta_data;
    
    /**
     * The schema objects hosted by the data source.
     */
    private Map<String, SchemaObject>     m_schema_objects;
    
    /**
     * The external functions provided by the data source.
     */
    private Map<String, ExternalFunction> m_functions = new HashMap<String, ExternalFunction>();
    
    /**
     * The state of the data source.
     */
    private DataSourceState               m_state;
    
    /**
     * The list of active transactions.
     */
    private List<DataSourceTransaction>   m_transactions;
    
    /**
     * Constructs an instance of a data source.
     * 
     * @param meta_data
     *            the data source meta data.
     */
    protected AbstractDataSource(DataSourceMetaData meta_data)
    {
        assert (meta_data != null);
        
        m_meta_data = meta_data;
        
        // We need a synchronized structure here since data sources are not transactional yet
        m_schema_objects = new HashMap<String, SchemaObject>();
        
        // We need a synchronized structure here since data sources are not transactional yet
        m_transactions = new Vector<DataSourceTransaction>();
        
        m_state = DataSourceState.CLOSED;
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        assert (m_state == DataSourceState.CLOSED);
        assert (m_transactions.size() == 0);
        
        m_state = DataSourceState.OPEN;
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        if (m_state == DataSourceState.CLOSED) return;
        
        if (m_transactions.size() > 0)
        {
            log.error("Encountered active transactions when closing data source: " + m_meta_data.getName());
        }
        
        // Rollback active transactions
        for (DataSourceTransaction transaction : new ArrayList<DataSourceTransaction>(m_transactions))
            transaction.rollback();
        
        m_state = DataSourceState.CLOSED;
    }
    
    @Override
    public DataSourceState getState()
    {
        return m_state;
    }
    
    @Override
    public DataSourceMetaData getMetaData()
    {
        return m_meta_data;
    }
    
    /**
     * Adds a transaction to the list of active ones.
     * 
     * @param transaction
     *            the transaction to add.
     */
    protected void addTransaction(DataSourceTransaction transaction)
    {
        assert (m_state == DataSourceState.OPEN);
        assert (!m_transactions.contains(transaction));
        assert (transaction.getState() == TransactionState.ACTIVE);
        
        m_transactions.add(transaction);
    }
    
    /**
     * Removes a committed or aborted transaction from to the list of active ones.
     * 
     * @param transaction
     *            the transaction to remove.
     */
    protected void removeTransaction(DataSourceTransaction transaction)
    {
        assert (m_state == DataSourceState.OPEN);
        assert (transaction.getState() == TransactionState.COMMITED || transaction.getState() == TransactionState.ABORTED);
        
        boolean existed = m_transactions.remove(transaction);
        assert (existed);
    }
    
    @Override
    public SchemaObject createSchemaObject(String name, SchemaTree schema, Size estimate) throws QueryExecutionException
    {
        assert (m_state == DataSourceState.OPEN);
        
        DataSourceTransaction transaction = this.beginTransaction();
        
        SchemaObject schema_obj = this.createSchemaObject(name, SchemaObjectScope.PERSISTENT, schema, estimate, transaction);
        
        transaction.commit();
        
        return schema_obj;
    }
    
    @Override
    public SchemaObject createSchemaObject(String name, SchemaObjectScope scope, SchemaTree schema, Size estimate,
            DataSourceTransaction transaction) throws QueryExecutionException
    {
        assert (m_state == DataSourceState.OPEN);
        
        // Create new schema object
        SchemaObject new_schema_obj = new SchemaObject(name, scope, schema, estimate);
        if (m_schema_objects.containsKey(name))
        {
            Throwable cause = new DataSourceException(ExceptionMessages.DataSource.INVALID_NEW_SCHEMA_OBJ_NAME, name,
                                                      m_meta_data.getName());
            
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.CREATE_SCHEMA_OBJ_ERROR, cause, name, m_meta_data.getName());
        }
        
        m_schema_objects.put(name, new_schema_obj);
        
        return new_schema_obj;
    }
    
    @Override
    public boolean hasSchemaObject(String name, DataSourceTransaction transaction)
    {
        assert (name != null);
        
        // Check if the schema object is temporary
        if (transaction.hasTemporarySchemaObject(name))
        {
            return true;
        }
        
        return m_schema_objects.containsKey(name);
    }
    
    @Override
    public boolean hasSchemaObject(String name)
    {
        assert (name != null);
        
        boolean out = false;
        try
        {
            DataSourceTransaction transaction = this.beginTransaction();
            
            out = this.hasSchemaObject(name, transaction);
            
            transaction.commit();
        }
        catch (QueryExecutionException e)
        {
            assert (false);
        }
        
        return out;
    }
    
    @Override
    public Collection<SchemaObject> getSchemaObjects()
    {
        return Collections.unmodifiableCollection(m_schema_objects.values());
    }
    
    /**
     * Declares the index in the schema tree.
     * 
     * @param data_obj_name
     *            the name of the data object.
     * @param name
     *            the name of the index name.
     * @param collection_path
     *            the path to the collection
     * @param key_paths
     *            the list of paths to the attributes as the key
     * @param unique
     *            indicates if the duplicates value of a key is allowed.
     * @param method
     *            the index method.
     * @return
     * @throws QueryExecutionException
     *             if anything wrong happens during the index declaration.
     */
    protected IndexDeclaration declareIndex(String data_obj_name, String name, SchemaPath collection_path,
            List<SchemaPath> key_paths, boolean unique, IndexMethod method) throws QueryExecutionException
    {
        SchemaTree schema_tree = getSchemaObject(data_obj_name).getSchemaTree();
        CollectionType collection_type = (CollectionType) collection_path.find(schema_tree);
        if (collection_type == null)
        {
            Throwable cause = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_COLLECTION_PATH,
                                                      collection_path.toString());
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.CREATE_INDEX_ERROR, cause, name, this.getMetaData().getName());
        }
        
        List<ScalarType> keys = new ArrayList<ScalarType>();
        for (SchemaPath key_path : key_paths)
        {
            ScalarType key = (ScalarType) key_path.find(collection_type);
            if (key == null)
            {
                Throwable cause = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_COLLECTION_PATH,
                                                          key_path.toString());
                // Chain the exception
                throw new QueryExecutionException(QueryExecution.CREATE_INDEX_ERROR, cause, name, this.getMetaData().getName());
            }
            keys.add(key);
        }
        // Adds the index declaration to the schema tree
        IndexDeclaration declaration = new IndexDeclaration(name, collection_type, keys, unique, method);
        try
        {
            collection_type.addIndex(declaration);
        }
        catch (DataSourceException e)
        {
            throw new QueryExecutionException(QueryExecution.CREATE_INDEX_ERROR, e, name, this.getMetaData().getName());
        }
        
        return declaration;
    }
    
    /**
     * Drops the index declaration in the schema tree.
     * 
     * @param data_obj_name
     *            the name of the data object.
     * @param name
     *            the name of the index.
     * @param collection_path
     *            the path to the collection
     * @exception QueryExecutionException
     *                if anything wrong happens during the index deletion.
     */
    protected void dropIndexDeclaration(String data_obj_name, String name, SchemaPath collection_path)
            throws QueryExecutionException
    {
        // Drop the Index in schema tree
        SchemaTree schema_tree = getSchemaObject(data_obj_name).getSchemaTree();
        CollectionType collection_type = (CollectionType) collection_path.find(schema_tree);
        if (collection_type == null)
        {
            Throwable cause = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_COLLECTION_PATH, data_obj_name
                    + collection_path.toString());
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.DELETE_INDEX_ERROR, cause, data_obj_name,
                                              this.getMetaData().getName());
        }
        try
        {
            collection_type.removeIndex(name);
        }
        catch (DataSourceException e)
        {
            throw new QueryExecutionException(QueryExecution.DELETE_INDEX_ERROR, name, getMetaData().getName());
        }
    }
    
    @Override
    public SchemaObject getSchemaObject(String name, DataSourceTransaction transaction) throws QueryExecutionException
    {
        // Check if the schema object is temporary
        if (transaction.hasTemporarySchemaObject(name))
        {
            return transaction.getTemporarySchemaObject(name);
        }
        else
        {
            return this.getSchemaObject(name);
        }
    }
    
    @Override
    public SchemaObject getSchemaObject(String name) throws QueryExecutionException
    {
        if (!m_schema_objects.containsKey(name))
        {
            Exception ex = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_SCHEMA_OBJ_NAME, name,
                                                   m_meta_data.getName());
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.GET_SCHEMA_OBJ_ERROR, ex, name, m_meta_data.getName());
        }
        
        return m_schema_objects.get(name);
    }
    
    @Override
    public void createIndex(String data_obj_name, String name, SchemaPath collection_path, List<SchemaPath> key_paths,
            boolean unique, IndexMethod method) throws QueryExecutionException
    {
        assert (m_state == DataSourceState.OPEN);
        
        DataSourceTransaction transaction = this.beginTransaction();
        createIndex(data_obj_name, name, collection_path, key_paths, unique, method, transaction);
        transaction.commit();
    }
    
    @Override
    public void deleteIndex(String data_obj_name, String name, SchemaPath collection_path) throws QueryExecutionException
    {
        assert (m_state == DataSourceState.OPEN);
        DataSourceTransaction transaction = this.beginTransaction();
        deleteIndex(data_obj_name, name, collection_path, transaction);
        transaction.commit();
    }
    
    @Override
    public List<TupleValue> getFromIndex(String data_obj_name, SchemaPath collection_path, String name, List<KeyRange> ranges)
            throws QueryExecutionException
    {
        assert (m_state == DataSourceState.OPEN);
        DataSourceTransaction transaction = this.beginTransaction();
        List<TupleValue> results = getFromIndex(data_obj_name, collection_path, name, ranges, transaction);
        transaction.commit();
        return results;
    }
    
    @Override
    public SchemaObject dropSchemaObject(String name) throws QueryExecutionException
    {
        assert (m_state == DataSourceState.OPEN);
        
        DataSourceTransaction transaction = this.beginTransaction();
        
        SchemaObject schema_obj = this.dropSchemaObject(name, transaction);
        
        transaction.commit();
        
        return schema_obj;
    }
    
    @Override
    public SchemaObject dropSchemaObject(String name, DataSourceTransaction transaction) throws QueryExecutionException
    {
        assert (this.getState() == DataSourceState.OPEN);
        assert (transaction.getState() == TransactionState.ACTIVE);
        
        if (!this.hasSchemaObject(name, transaction))
        {
            Throwable cause = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_SCHEMA_OBJ_NAME, name,
                                                      this.getMetaData().getName());
            
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.DROP_SCHEMA_OBJ_ERROR, cause, name, this.getMetaData().getName());
        }
        
        // Check if the schema object is temporary
        if (transaction.hasTemporarySchemaObject(name))
        {
            return transaction.removeTemporarySchemaObject(name);
        }
        else
        {
            SchemaObject schema_obj = m_schema_objects.remove(name);
            
            return schema_obj;
        }
    }
    
    @Override
    public DataTree getDataObject(String name) throws QueryExecutionException
    {
        assert (m_state == DataSourceState.OPEN);
        
        DataSourceTransaction transaction = this.beginTransaction();
        
        DataTree data_obj = this.getDataObject(name, transaction);
        
        transaction.commit();
        
        return data_obj;
    }
    
    @Override
    public void setDataObject(String name, DataTree data_tree) throws QueryExecutionException
    {
        assert (m_state == DataSourceState.OPEN);
        
        DataSourceTransaction transaction = this.beginTransaction();
        
        this.setDataObject(name, data_tree, transaction);
        
        transaction.commit();
    }
    
    @Override
    public void deleteDataObject(String name) throws QueryExecutionException
    {
        assert (m_state == DataSourceState.OPEN);
        
        DataSourceTransaction transaction = this.beginTransaction();
        
        this.deleteDataObject(name, transaction);
        
        transaction.commit();
    }
    
    @Override
    public void addExternalFunction(ExternalFunction function)
    {
        assert (function != null);
        
        assert (function.getTargetDataSource().equals(m_meta_data.getName()));
        
        String name = function.getName().toUpperCase();
        
        assert !(m_functions.containsKey(name));
        m_functions.put(name, function);
    }
    
    @Override
    public ExternalFunction getExternalFunction(String name) throws FunctionRegistryException
    {
        if (!m_functions.containsKey(name.toUpperCase()))
        {
            throw new FunctionRegistryException(ExceptionMessages.Function.UNKNOWN_FUNCTION_NAME, m_meta_data.getName()
                    + QueryPath.PATH_SEPARATOR + name);
        }
        
        return m_functions.get(name.toUpperCase());
    }
    
    @Override
    public QueryResult execute(PhysicalPlan physical_plan) throws QueryExecutionException
    {
        assert (m_state == DataSourceState.OPEN);
        
        DataSourceTransaction transaction = this.beginTransaction();
        
        QueryResult result = this.execute(physical_plan, transaction);
        
        transaction.commit();
        
        return result;
    }
    
    @Override
    public void clear() throws QueryExecutionException
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public String toString()
    {
        StringBuilder b = new StringBuilder();
        b.append("Data source: " + m_meta_data.getName() + "\n");
        for (SchemaObject so : m_schema_objects.values())
        {
            b.append("Schema object: ");
            b.append(so.getName());
            b.append("\n");
            b.append(so.getSchemaTree());
            b.append("\n");
        }
        return b.toString();
    }
}
