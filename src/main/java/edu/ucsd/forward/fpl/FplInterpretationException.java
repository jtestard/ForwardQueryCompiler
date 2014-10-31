/**
 * 
 */
package edu.ucsd.forward.fpl;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.CheckedException;
import edu.ucsd.forward.exception.ExceptionMessages.ActionInterpretation;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryExecutionException;

/**
 * Represents an exception thrown during action interpretation.
 * 
 * @author Yupeng
 * 
 */
public class FplInterpretationException extends CheckedException
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
    public FplInterpretationException(ActionInterpretation message, Object... params)
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
    public FplInterpretationException(ActionInterpretation message, Throwable cause, Object... params)
    {
        super(message, cause, params);
    }
    
    /**
     * Constructs an exception with a given message, possibly parameterized.
     * 
     * @param message
     *            the exception message
     * @param params
     *            the parameters that the message is expecting
     */
    public FplInterpretationException(QueryExecution message, Object... params)
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
    public FplInterpretationException(QueryExecution message, Throwable cause, Object... params)
    {
        super(message, cause, params);
    }
}
