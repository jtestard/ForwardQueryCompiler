/**
 * 
 */
package edu.ucsd.forward.data.source.idb;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.AbstractDataSourceTransaction;
import edu.ucsd.forward.data.source.DataSource;

/**
 * An indexedDB transaction.
 * 
 * @author Yupeng
 * 
 */
public class IndexedDbTransaction extends AbstractDataSourceTransaction
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(IndexedDbTransaction.class);
    
    private IndexedDbDataSource m_data_source;
    
    /**
     * Constructs an indexedDB transaction.
     * 
     * @param data_source
     *            the indexedDB data source.
     */
    public IndexedDbTransaction(IndexedDbDataSource data_source)
    {
        assert data_source != null;
        m_data_source = data_source;
        
        // Begin the transaction
        super.begin();
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward._new.data.source.DataSourceTransaction#getDataSource()
     */
    @Override
    public DataSource getDataSource()
    {
        return m_data_source;
    }
}
