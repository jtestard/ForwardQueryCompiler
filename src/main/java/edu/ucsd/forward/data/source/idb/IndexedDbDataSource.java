/**
 * 
 */
package edu.ucsd.forward.data.source.idb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.index.Index;
import edu.ucsd.forward.data.index.IndexDeclaration;
import edu.ucsd.forward.data.index.IndexMethod;
import edu.ucsd.forward.data.index.KeyRange;
import edu.ucsd.forward.data.source.AbstractDataSource;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.source.DataSourceTransaction;
import edu.ucsd.forward.data.source.SchemaObject;
import edu.ucsd.forward.data.source.DataSourceMetaData.DataModel;
import edu.ucsd.forward.data.source.DataSourceMetaData.StorageSystem;
import edu.ucsd.forward.data.source.DataSourceTransaction.TransactionState;
import edu.ucsd.forward.data.source.SchemaObject.SchemaObjectScope;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.exception.ExceptionMessages;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.LazyQueryResult;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryResult;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * The indexedDB data source that provides synchronous operation on its cache.
 * 
 * @author Yupeng
 * 
 */
public class IndexedDbDataSource extends AbstractDataSource
{
    @SuppressWarnings("unused")
    private static final Logger   log = Logger.getLogger(IndexedDbDataSource.class);
    
    /**
     * A cache of the data objects stored in the indexedDB source.
     */
    private Map<String, DataTree> m_data_objects;
    
    /**
     * Constructs an instance of a memory data source.
     * 
     * @param name
     *            name of the in-memory data source.
     * @param model
     *            the data model of the in-memory data source.
     */
    public IndexedDbDataSource(String name, DataModel model)
    {
        super(new DataSourceMetaData(name, model, StorageSystem.INDEXEDDB));
        m_data_objects = new HashMap<String, DataTree>();
    }
    
    @Override
    public SchemaObject createSchemaObject(String name, SchemaObjectScope scope, SchemaTree schema, Size estimate,
            DataSourceTransaction transaction) throws QueryExecutionException
    {
        SchemaObject schema_obj = super.createSchemaObject(name, scope, schema, estimate, transaction);
        
        // Initialize a data tree for the new schema object
        DataTree data_tree = new DataTree(schema.getRootType().getDefaultValue());
        this.setDataObject(schema_obj.getName(), data_tree);
        
        return schema_obj;
    }
    
    @Override
    public DataSourceTransaction beginTransaction()
    {
        assert (this.getState() == DataSourceState.OPEN);
        
        return new IndexedDbTransaction(this);
    }
    
    @Override
    public void setDataObject(String name, DataTree data_tree, DataSourceTransaction transaction) throws QueryExecutionException
    {
        assert (this.getState() == DataSourceState.OPEN);
        assert (transaction instanceof IndexedDbTransaction);
        assert (transaction.getState() == TransactionState.ACTIVE);
        
        if (!this.hasSchemaObject(name, transaction))
        {
            Throwable cause = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_DATA_OBJ_NAME, name,
                                                      this.getMetaData().getName());
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.SET_DATA_OBJ_ERROR, cause, name, this.getMetaData().getName());
        }
        
        if (data_tree != null)
        {
            m_data_objects.put(name, data_tree);
        }
    }
    
    @Override
    public DataTree getDataObject(String name, DataSourceTransaction transaction) throws QueryExecutionException
    {
        assert (this.getState() == DataSourceState.OPEN);
        
        if (!this.hasSchemaObject(name, transaction))
        {
            Throwable cause = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_DATA_OBJ_NAME, name,
                                                      this.getMetaData().getName());
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.GET_DATA_OBJ_ERROR, cause, name, this.getMetaData().getName());
        }
        
        DataTree data_tree = m_data_objects.get(name);
        
        assert (data_tree != null);
        
        return data_tree;
    }
    
    @Override
    public void deleteDataObject(String name, DataSourceTransaction transaction) throws QueryExecutionException
    {
        assert (this.getState() == DataSourceState.OPEN);
        assert (transaction instanceof IndexedDbTransaction);
        assert (transaction.getState() == TransactionState.ACTIVE);
        
        if (!this.hasSchemaObject(name, transaction))
        {
            Throwable cause = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_DATA_OBJ_NAME, name,
                                                      this.getMetaData().getName());
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.DELETE_DATA_OBJ_ERROR, cause, name, this.getMetaData().getName());
        }
        
        // Remove the data object entry
        m_data_objects.remove(name);
    }
    
    @Override
    public QueryResult execute(PhysicalPlan physical_plan, DataSourceTransaction transaction)
    {
        assert (this.getState() == DataSourceState.OPEN);
        assert (transaction instanceof IndexedDbTransaction);
        assert (transaction.getState() == TransactionState.ACTIVE);
        
        // Construct an IndexedDB result
        IndexedDbResult result = new IndexedDbResult(physical_plan);
        
        return new LazyQueryResult(result, physical_plan);
    }
    
    @Override
    public String toString()
    {
        StringBuilder b = new StringBuilder();
        b.append("IndexedDB data source: " + this.getMetaData().getName() + "\n");
        for (SchemaObject so : this.getSchemaObjects())
        {
            String name = so.getName();
            b.append("Schema object: ");
            b.append(name);
            b.append("\n");
            b.append(so.getSchemaTree());
            if (m_data_objects.containsKey(name))
            {
                b.append(m_data_objects.get(name).toString());
                b.append("\n");
            }
        }
        return b.toString();
    }
    
    @Override
    public void createIndex(String data_obj_name, String name, SchemaPath collection_path, List<SchemaPath> key_paths,
            boolean unique, IndexMethod method, DataSourceTransaction transaction) throws QueryExecutionException
    {
        assert (this.getState() == DataSourceState.OPEN);
        assert (transaction instanceof IndexedDbTransaction);
        assert (transaction.getState() == TransactionState.ACTIVE);
        
        IndexDeclaration declaration = declareIndex(data_obj_name, name, collection_path, key_paths, unique, method);
        
        // Builds the index in the data tree.
        DataTree data_tree = getDataObject(data_obj_name).getDataTree();
        try
        {
            data_tree.createIndex(declaration);
        }
        catch (DataSourceException e)
        {
            throw new QueryExecutionException(QueryExecution.CREATE_INDEX_ERROR, name, getMetaData().getName());
        }
    }
    
    @Override
    public void deleteIndex(String data_obj_name, String name, SchemaPath collection_path, DataSourceTransaction transaction)
            throws QueryExecutionException
    {
        assert (this.getState() == DataSourceState.OPEN);
        assert (transaction instanceof IndexedDbTransaction);
        assert (transaction.getState() == TransactionState.ACTIVE);
        
        dropIndexDeclaration(data_obj_name, name, collection_path);
        
        // Drop the index in data tree.
        DataTree data_tree = getDataObject(data_obj_name).getDataTree();
        try
        {
            data_tree.deleteIndex(collection_path, name);
        }
        catch (DataSourceException e)
        {
            throw new QueryExecutionException(QueryExecution.DELETE_INDEX_ERROR, name, getMetaData().getName());
        }
    }
    
    @Override
    public List<TupleValue> getFromIndex(String data_obj_name, SchemaPath collection_path, String name, List<KeyRange> ranges,
            DataSourceTransaction transaction) throws QueryExecutionException
    {
        assert (this.getState() == DataSourceState.OPEN);
        assert (transaction instanceof IndexedDbTransaction);
        assert (transaction.getState() == TransactionState.ACTIVE);
        // Get the data tree from cache.
        DataTree data_tree = getDataObject(data_obj_name).getDataTree();
        Index index;
        try
        {
            index = data_tree.getIndex(collection_path, name);
        }
        catch (DataSourceException e)
        {
            throw new QueryExecutionException(QueryExecution.ACCESS_INDEX_ERROR, e, name, getMetaData().getName());
        }
        return index.get(ranges);
    }
    
}
