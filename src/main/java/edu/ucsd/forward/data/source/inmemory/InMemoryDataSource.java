package edu.ucsd.forward.data.source.inmemory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.index.Index;
import edu.ucsd.forward.data.index.IndexDeclaration;
import edu.ucsd.forward.data.index.IndexMethod;
import edu.ucsd.forward.data.index.KeyRange;
import edu.ucsd.forward.data.source.AbstractDataSource;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.source.DataSourceMetaData.DataModel;
import edu.ucsd.forward.data.source.DataSourceMetaData.Site;
import edu.ucsd.forward.data.source.DataSourceMetaData.StorageSystem;
import edu.ucsd.forward.data.source.DataSourceTransaction;
import edu.ucsd.forward.data.source.DataSourceTransaction.TransactionState;
import edu.ucsd.forward.data.source.SchemaObject;
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
 * Represents a data source that stores schema and data objects in memory.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 */
public class InMemoryDataSource extends AbstractDataSource
{
    /**
     * The data objects hosted by of the data source.
     */
    private Map<String, DataTree> m_data_objects;
    
    /**
     * Constructs an instance of a memory data source.
     * 
     * @param name
     *            name of the in-memory data source.
     * @param model
     *            the data model of the in-memory data source.
     * @param site
     *            the site of the in-memory date source.
     */
    public InMemoryDataSource(String name, DataModel model, Site site)
    {
        super(new DataSourceMetaData(name, model, StorageSystem.INMEMORY, site));
        m_data_objects = new HashMap<String, DataTree>();
    }
    
    /**
     * Constructs an instance of a memory data source with the Site set to SERVER by default.
     * 
     * @param name
     *            name of the in-memory data source.
     * @param model
     *            the data model of the in-memory data source.
     */
    public InMemoryDataSource(String name, DataModel model)
    {
        super(new DataSourceMetaData(name, model, StorageSystem.INMEMORY));
        m_data_objects = new HashMap<String, DataTree>();
    }
    
    @Override
    public InMemoryTransaction beginTransaction()
    {
        assert (this.getState() == DataSourceState.OPEN);
        
        return new InMemoryTransaction(this);
    }
    
    @Override
    public SchemaObject createSchemaObject(String name, SchemaObjectScope scope, SchemaTree schema, Size estimate,
            DataSourceTransaction transaction) throws QueryExecutionException
    {
        SchemaObject schema_obj = null;
        switch (scope)
        {
            case TEMPORARY:
                // Create new schema object
                schema_obj = new SchemaObject(name, scope, schema, estimate);
                // Add the temporary object to the transaction
                ((InMemoryTransaction) transaction).addTemporarySchemaObject(schema_obj);
                break;
            case PERSISTENT:
                schema_obj = super.createSchemaObject(name, scope, schema, estimate, transaction);
                break;
            default:
                throw new UnsupportedOperationException();
        }
        
        // Initialize a data tree for the new schema object
        DataTree data_tree = new DataTree(
                                          ValueUtil.cloneNoParentNoType(schema_obj.getSchemaTree().getRootType().getDefaultValue()));
        this.setDataObject(schema_obj.getName(), data_tree, transaction);
        
        return schema_obj;
    }
    
    @Override
    public SchemaObject dropSchemaObject(String name, DataSourceTransaction transaction) throws QueryExecutionException
    {
        assert (this.getState() == DataSourceState.OPEN);
        assert (transaction instanceof InMemoryTransaction);
        assert (transaction.getState() == TransactionState.ACTIVE);
        
        // Check if the schema object is temporary
        if (transaction.hasTemporarySchemaObject(name))
        {
            if (((InMemoryTransaction) transaction).hasTemporaryDataObject(name))
            {
                ((InMemoryTransaction) transaction).removeTemporaryDataObject(name);
            }
            
            return transaction.removeTemporarySchemaObject(name);
        }
        else
        {
            if (m_data_objects.containsKey(name))
            {
                this.deleteDataObject(name, transaction);
            }
            
            return super.dropSchemaObject(name, transaction);
        }
    }
    
    @Override
    public void setDataObject(String name, DataTree data_tree, DataSourceTransaction transaction) throws QueryExecutionException
    {
        assert (this.getState() == DataSourceState.OPEN);
        assert (transaction instanceof InMemoryTransaction);
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
            // Check if the schema object is temporary
            if (transaction.hasTemporarySchemaObject(name))
            {
                ((InMemoryTransaction) transaction).addTemporaryDataObject(name, data_tree);
            }
            else
            {
                m_data_objects.put(name, data_tree);
                data_tree.setDataSource(this);
            }
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
        
        // Check if the schema object is temporary
        DataTree data_tree = null;
        if (transaction.hasTemporarySchemaObject(name))
        {
            data_tree = ((InMemoryTransaction) transaction).getTemporaryDataObject(name);
        }
        else
        {
            data_tree = m_data_objects.get(name);
        }
        
        assert (data_tree != null);
        
        return data_tree;
    }
    
    @Override
    public void deleteDataObject(String name, DataSourceTransaction transaction) throws QueryExecutionException
    {
        assert (this.getState() == DataSourceState.OPEN);
        assert (transaction instanceof InMemoryTransaction);
        assert (transaction.getState() == TransactionState.ACTIVE);
        
        if (!this.hasSchemaObject(name, transaction))
        {
            Throwable cause = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_DATA_OBJ_NAME, name,
                                                      this.getMetaData().getName());
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.DELETE_DATA_OBJ_ERROR, cause, name, this.getMetaData().getName());
        }
        
        // Check if the schema object is temporary
        if (transaction.hasTemporarySchemaObject(name))
        {
            ((InMemoryTransaction) transaction).removeTemporaryDataObject(name);
        }
        else
        {
            // Remove the data object entry
            DataTree data_tree = m_data_objects.remove(name);
            data_tree.setDataSource(null);
        }
    }
    
    @Override
    public QueryResult execute(PhysicalPlan physical_plan, DataSourceTransaction transaction)
    {
        assert (this.getState() == DataSourceState.OPEN);
        assert (physical_plan != null);
        assert (transaction instanceof InMemoryTransaction);
        assert (transaction.getState() == TransactionState.ACTIVE);
        
        return new LazyQueryResult(physical_plan, physical_plan);
    }
    
    @Override
    public String toString()
    {
        StringBuilder b = new StringBuilder();
        b.append("Data source: " + this.getMetaData().getName() + "\n");
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
        assert (transaction instanceof InMemoryTransaction);
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
        assert (transaction instanceof InMemoryTransaction);
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
        assert (transaction instanceof InMemoryTransaction);
        assert (transaction.getState() == TransactionState.ACTIVE);
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
    
    @Override
    public void clear() throws QueryExecutionException
    {
        for (SchemaObject schema_object : new ArrayList<SchemaObject>(getSchemaObjects()))
        {
            dropSchemaObject(schema_object.getName());
        }
    }
}
