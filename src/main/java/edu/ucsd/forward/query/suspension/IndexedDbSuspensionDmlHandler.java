/**
 * 
 */
package edu.ucsd.forward.query.suspension;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.idb.IndexedDbExecution;
import edu.ucsd.forward.query.QueryExecutionException;

/**
 * The suspension handler for indexedDB suspension request.
 * 
 * @author Yupeng
 * 
 */
public class IndexedDbSuspensionDmlHandler extends AbstractSuspensionHandler<IndexedDbSuspensionDmlRequest>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(IndexedDbSuspensionDmlHandler.class);
    
    /**
     * Constructs the indexedDB suspension handler with the request.
     * 
     * @param request
     *            the indexedDB suspension request.
     */
    protected IndexedDbSuspensionDmlHandler(IndexedDbSuspensionDmlRequest request)
    {
        super(request);
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * 
     * @see edu.ucsd.forward._new.query.suspension.SuspensionHandler#handle(edu.ucsd.forward._new.query.suspension.SuspensionRequest)
     */
    @Override
    public void handle() throws QueryExecutionException
    {
        IndexedDbSuspensionDmlRequest request = getRequest();
        
        IndexedDbExecution.executeUpdate(request.getDmlOperator(), request.getPayload());
    }
}
