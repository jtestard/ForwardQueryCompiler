/**
 * 
 */
package edu.ucsd.forward.query.physical;

import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * The pipeline interface.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface Pipeline
{
    /**
     * Opens the pipeline, which can then start producing values. The pipeline can be reopened.
     * 
     * @throws QueryExecutionException
     *             if the pipeline fails to open.
     */
    public abstract void open() throws QueryExecutionException;
    
    /**
     * Gets the next value produced by the pipeline.
     * 
     * @return a binding; or null if there is no next binding.
     * @throws QueryExecutionException
     *             if the pipeline fails to compute the next value.
     * @throws SuspensionException
     *             if the pipeline needs to suspend.
     */
    public abstract Binding next() throws QueryExecutionException, SuspensionException;
    
    /**
     * Closes the pipeline, which stops producing values. The pipeline can already be closed.
     * 
     * @throws QueryExecutionException
     *             if the pipeline fails to close.
     */
    public abstract void close() throws QueryExecutionException;
    
}
