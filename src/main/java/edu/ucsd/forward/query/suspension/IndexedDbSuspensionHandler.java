/**
 * 
 */
package edu.ucsd.forward.query.suspension;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.idb.IndexedDbExecution;
import edu.ucsd.forward.data.source.idb.IndexedDbResult;
import edu.ucsd.forward.query.QueryExecutionException;

/**
 * The suspension handler for indexedDB suspension request.
 * 
 * @author Yupeng
 * 
 */
public class IndexedDbSuspensionHandler extends AbstractSuspensionHandler<IndexedDbSuspensionRequest>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(IndexedDbSuspensionHandler.class);
    
    /**
     * Constructs the indexedDB suspension handler with the request.
     * 
     * @param request
     *            the indexedDB suspension request.
     */
    protected IndexedDbSuspensionHandler(IndexedDbSuspensionRequest request)
    {
        super(request);
    }
    
    @Override
    public void handle() throws QueryExecutionException
    {
        IndexedDbSuspensionRequest request = getRequest();
        
        IndexedDbResult result = request.getIndexedDbResult();
        IndexedDbExecution.execute(result);
    }
}
