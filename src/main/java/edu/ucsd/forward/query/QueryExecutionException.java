/**
 * 
 */
package edu.ucsd.forward.query;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.fpl.FplInterpretationException;

/**
 * Represents an exception thrown during query execution.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class QueryExecutionException extends FplInterpretationException
{
    private static final long   serialVersionUID = 1L;
    
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(QueryExecutionException.class);
    
    /**
     * Constructs an exception with a given message, possibly parameterized.
     * 
     * @param message
     *            the exception message
     * @param params
     *            the parameters that the message is expecting
     */
    public QueryExecutionException(QueryExecution message, Object... params)
    {
        super(message, params);
    }
    
    /**
     * Constructs an exception with a given message, possibly parameterized.
     * 
     * @param message
     *            the exception message
     * @param cause
     *            the cause of this exception in case of chained exceptions
     * @param params
     *            the parameters that the message is expecting
     */
    public QueryExecutionException(QueryExecution message, Throwable cause, Object... params)
    {
        super(message, cause, params);
    }
    
}
