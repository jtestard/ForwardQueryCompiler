/**
 * 
 */
package edu.ucsd.forward.query.suspension;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.idb.IndexedDbResult;

/**
 * 
 * The suspension request for indexedDB data source.
 * 
 * @author Yupeng
 * 
 */
public class IndexedDbSuspensionRequest implements SuspensionRequest
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(IndexedDbSuspensionRequest.class);
    
    private IndexedDbResult     m_result;
    
    /**
     * Constructs the indexedDB suspension request.
     * 
     * @param result
     *            the indexedDB result that requires asynchronous execution.
     */
    public IndexedDbSuspensionRequest(IndexedDbResult result)
    {
        assert result != null;
        m_result = result;
    }
    
    /**
     * Gets the indexedDB result that throws the request.
     * 
     * @return the indexedDB result that throws the request.
     */
    public IndexedDbResult getIndexedDbResult()
    {
        return m_result;
    }
}
