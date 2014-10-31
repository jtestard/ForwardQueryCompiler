/**
 * 
 */
package edu.ucsd.forward.data.source.inmemory;

import java.util.HashMap;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.AbstractDataSourceTransaction;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.query.QueryExecutionException;

/**
 * An in-memory transaction.
 * 
 * @author Michalis Petropoulos
 */
public class InMemoryTransaction extends AbstractDataSourceTransaction
{
    @SuppressWarnings("unused")
    private static final Logger   log = Logger.getLogger(InMemoryTransaction.class);
    
    private InMemoryDataSource    m_data_source;
    
    /**
     * The data objects hosted by of the data source.
     */
    private Map<String, DataTree> m_temp_data_objects;
    
    /**
     * Constructs an in-memory transaction.
     * 
     * @param data_source
     *            the in-memory data source.
     */
    public InMemoryTransaction(InMemoryDataSource data_source)
    {
        assert (data_source != null);
        m_data_source = data_source;
        m_temp_data_objects = new HashMap<String, DataTree>();
        
        // Begin the transaction
        super.begin();
    }
    
    @Override
    public InMemoryDataSource getDataSource()
    {
        return m_data_source;
    }
    
    @Override
    public void commit() throws QueryExecutionException
    {
        super.commit();
    }
    
    @Override
    public void rollback() throws QueryExecutionException
    {
        super.rollback();
    }
    
    /**
     * Adds a temporary data object to the connection.
     * 
     * @param name
     *            the name of the temporary data object to add.
     * @param data_tree
     *            the data tree of the temporary data object.
     * @exception QueryExecutionException
     *                if there is no temporary schema object with the given name.
     */
    public void addTemporaryDataObject(String name, DataTree data_tree) throws QueryExecutionException
    {
        m_temp_data_objects.put(name, data_tree);
    }
    
    /**
     * Removes a temporary data object from the connection.
     * 
     * @param name
     *            the name of the temporary data object to remove.
     * @exception QueryExecutionException
     *                if there is no temporary schema object with the given name.
     */
    public void removeTemporaryDataObject(String name) throws QueryExecutionException
    {
        assert (m_temp_data_objects.containsKey(name));
        
        m_temp_data_objects.remove(name);
    }
    
    /**
     * Gets a temporary data object in the connection.
     * 
     * @param name
     *            the name of the temporary data object to get.
     * @return the data tree of a data object.
     * @exception QueryExecutionException
     *                if there is no temporary schema object with the given name.
     */
    public DataTree getTemporaryDataObject(String name) throws QueryExecutionException
    {
        return m_temp_data_objects.get(name);
    }
    
    /**
     * Determines whether the transaction has a temporary data object or not.
     * 
     * @param name
     *            the name of the temporary data object to get.
     * @return true, if the transaction has a temporary data object with the given name; otherwise, false.
     * @exception QueryExecutionException
     *                if there is no temporary schema object with the given name.
     */
    public boolean hasTemporaryDataObject(String name) throws QueryExecutionException
    {
        return m_temp_data_objects.containsKey(name);
    }
    
}
