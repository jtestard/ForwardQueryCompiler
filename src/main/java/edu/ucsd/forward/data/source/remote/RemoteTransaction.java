/**
 * 
 */
package edu.ucsd.forward.data.source.remote;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.AbstractDataSourceTransaction;
import edu.ucsd.forward.data.source.DataSource;

/**
 * A transaction on operating on remote data source.
 * 
 * @author Yupeng
 * 
 */
public class RemoteTransaction extends AbstractDataSourceTransaction
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(RemoteTransaction.class);
    
    private RemoteDataSource    m_data_source;
    
    /**
     * Constructs a remote transaction.
     * 
     * @param data_source
     *            the remote data source.
     */
    public RemoteTransaction(RemoteDataSource data_source)
    {
        assert data_source != null;
        m_data_source = data_source;
        
        // Begin the transaction
        super.begin();
    }
    
    @Override
    public DataSource getDataSource()
    {
        return m_data_source;
    }
}
