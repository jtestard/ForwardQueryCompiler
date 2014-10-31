/**
 * 
 */
package edu.ucsd.forward.data.source.remote;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.index.IndexMethod;
import edu.ucsd.forward.data.index.KeyRange;
import edu.ucsd.forward.data.source.AbstractDataSource;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.source.DataSourceTransaction;
import edu.ucsd.forward.data.source.SchemaObject;
import edu.ucsd.forward.data.source.DataSourceMetaData.DataModel;
import edu.ucsd.forward.data.source.DataSourceMetaData.StorageSystem;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryResult;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * The remote data source that provides synchronous operations.
 * 
 * @author Yupeng
 * 
 */
public class RemoteDataSource extends AbstractDataSource
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(RemoteDataSource.class);
    
    /**
     * Constructs the remote data source.
     * 
     * @param name
     *            the name of the data source.
     * @param model
     *            the data model.
     */
    public RemoteDataSource(String name, DataModel model)
    {
        super(new DataSourceMetaData(name, model, StorageSystem.REMOTE));
    }
    
    @Override
    public DataSourceTransaction beginTransaction() throws QueryExecutionException
    {
        assert (this.getState() == DataSourceState.OPEN);
        return new RemoteTransaction(this);
        
    }
    
    @Override
    public void createIndex(String dataObjName, String name, SchemaPath collectionPath, List<SchemaPath> keyPaths, boolean unique,
            IndexMethod method, DataSourceTransaction transaction) throws QueryExecutionException
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void deleteDataObject(String name, DataSourceTransaction transaction) throws QueryExecutionException
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void deleteIndex(String data_obj_name, String name, SchemaPath collection_path, DataSourceTransaction transaction)
            throws QueryExecutionException
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public QueryResult execute(PhysicalPlan physical_plan, DataSourceTransaction transaction) throws QueryExecutionException
    {
        // TODO Auto-generated method stub
        return null;
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.source.DataSource#getDataObject(java.lang.String,
     *      edu.ucsd.forward.data.source.DataSourceTransaction)
     */
    @Override
    public DataTree getDataObject(String name, DataSourceTransaction transaction) throws QueryExecutionException
    {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public List<TupleValue> getFromIndex(String data_obj_name, SchemaPath collection_path, String name, List<KeyRange> ranges,
            DataSourceTransaction transaction) throws QueryExecutionException
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void setDataObject(String name, DataTree data_tree, DataSourceTransaction transaction) throws QueryExecutionException
    {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public String toString()
    {
        StringBuilder b = new StringBuilder();
        b.append("Remote data source: " + this.getMetaData().getName() + "\n");
        for (SchemaObject so : this.getSchemaObjects())
        {
            String name = so.getName();
            b.append("Schema object: ");
            b.append(name);
            b.append("\n");
            b.append(so.getSchemaTree());
        }
        return b.toString();
    }
}
