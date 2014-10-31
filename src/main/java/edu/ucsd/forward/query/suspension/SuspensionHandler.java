/**
 * 
 */
package edu.ucsd.forward.query.suspension;

import edu.ucsd.forward.query.QueryExecutionException;


/**
 * A suspension handler that handles a suspension request and performs the job.
 * 
 * @author Yupeng
 * 
 */
public interface SuspensionHandler
{
    /**
     * Handles the suspension request.
     * 
     * @throws QueryExecutionException
     *             if anything goes wrong during the handling.
     */
    void handle() throws QueryExecutionException;
}
