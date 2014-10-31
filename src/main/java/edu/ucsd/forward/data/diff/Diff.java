/**
 * 
 */
package edu.ucsd.forward.data.diff;

import edu.ucsd.forward.data.DataPath;
import edu.ucsd.forward.data.value.Value;

/**
 * A data diff.
 * 
 * @author Kian Win Ong
 * 
 */
public interface Diff
{
    /**
     * Returns the context.
     * 
     * @return the context.
     */
    public DataPath getContext();
    
    /**
     * Returns the payload.
     * 
     * @return the payload.
     */
    public Value getPayload();
}
