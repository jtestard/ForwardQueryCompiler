/**
 * 
 */
package edu.ucsd.forward.data.source.asterix;

import java.util.List;
import java.util.Properties;

import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.index.IndexMethod;
import edu.ucsd.forward.data.index.KeyRange;
import edu.ucsd.forward.data.source.AbstractDataSource;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.source.DataSourceTransaction;
import edu.ucsd.forward.data.source.DataSourceMetaData.StorageSystem;
import edu.ucsd.forward.data.source.jdbc.JdbcDataSourceMetaData;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryResult;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.app2you.util.logger.Logger;

/**
 * This is the data source for AsterixDB clusters.
 * 
 * @author Jules Testard
 * 
 */
public class AsterixDataSource extends AbstractDataSource
{
    /**
     * Constructor for the AsterixDB data source.
     * @param name
     * @param connection_properties
     * @param overwrite
     */
    public AsterixDataSource(String name, Properties connection_properties, boolean overwrite)
    {
        super(null);
    }
    
    /**
     * @param meta_data
     */
    protected AsterixDataSource(DataSourceMetaData meta_data)
    {
        super(meta_data);
        // TODO Auto-generated constructor stub
    }
    
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AsterixDataSource.class);
    
    /**
     * <b>Inherited JavaDoc:</b><br>
     * {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.source.DataSource#beginTransaction()
     */
    @Override
    public DataSourceTransaction beginTransaction() throws QueryExecutionException
    {
        // TODO Auto-generated method stub
        return null;
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br>
     * {@inheritDoc} <br>
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
    
    /**
     * <b>Inherited JavaDoc:</b><br>
     * {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.source.DataSource#createIndex(java.lang.String, java.lang.String,
     *      edu.ucsd.forward.data.SchemaPath, java.util.List, boolean, edu.ucsd.forward.data.index.IndexMethod,
     *      edu.ucsd.forward.data.source.DataSourceTransaction)
     */
    @Override
    public void createIndex(String data_obj_name, String name, SchemaPath collection_path, List<SchemaPath> key_paths,
            boolean unique, IndexMethod method, DataSourceTransaction transaction) throws QueryExecutionException
    {
        // TODO Auto-generated method stub
        
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br>
     * {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.source.DataSource#deleteIndex(java.lang.String, java.lang.String,
     *      edu.ucsd.forward.data.SchemaPath, edu.ucsd.forward.data.source.DataSourceTransaction)
     */
    @Override
    public void deleteIndex(String data_obj_name, String name, SchemaPath collection_path, DataSourceTransaction transaction)
            throws QueryExecutionException
    {
        // TODO Auto-generated method stub
        
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br>
     * {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.source.DataSource#getFromIndex(java.lang.String, edu.ucsd.forward.data.SchemaPath,
     *      java.lang.String, java.util.List, edu.ucsd.forward.data.source.DataSourceTransaction)
     */
    @Override
    public List<TupleValue> getFromIndex(String data_obj_name, SchemaPath collection_path, String name, List<KeyRange> ranges,
            DataSourceTransaction transaction) throws QueryExecutionException
    {
        // TODO Auto-generated method stub
        return null;
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br>
     * {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.source.DataSource#setDataObject(java.lang.String, edu.ucsd.forward.data.value.DataTree,
     *      edu.ucsd.forward.data.source.DataSourceTransaction)
     */
    @Override
    public void setDataObject(String name, DataTree data_tree, DataSourceTransaction transaction) throws QueryExecutionException
    {
        // TODO Auto-generated method stub
        
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br>
     * {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.source.DataSource#deleteDataObject(java.lang.String,
     *      edu.ucsd.forward.data.source.DataSourceTransaction)
     */
    @Override
    public void deleteDataObject(String name, DataSourceTransaction transaction) throws QueryExecutionException
    {
        // TODO Auto-generated method stub
        
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br>
     * {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.source.DataSource#execute(edu.ucsd.forward.query.physical.plan.PhysicalPlan,
     *      edu.ucsd.forward.data.source.DataSourceTransaction)
     */
    @Override
    public QueryResult execute(PhysicalPlan physical_plan, DataSourceTransaction transaction) throws QueryExecutionException
    {
        // TODO Auto-generated method stub
        return null;
    }
}
