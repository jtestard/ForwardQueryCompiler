/**
 * 
 */
package edu.ucsd.forward.exception;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * Represents unchecked exceptions. Each unchecked exception is associated with a message in ExceptionMessages, which contains
 * display templates for exceptions, and the exceptions provide the actual parameters for the templates.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class UncheckedException extends RuntimeException
{
    private static final long   serialVersionUID = 1L;
    
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(UncheckedException.class);
    
    /**
     * Constructs an exception with a given message.
     * 
     * @param message
     *            the exception message
     */
    public UncheckedException(String message)
    {
        this(message, null);
    }
    
    /**
     * Constructs an exception.
     * 
     * @param cause
     *            the cause of this exception in case of chained exceptions
     */
    public UncheckedException(Throwable cause)
    {
        super(cause);
    }
    
    /**
     * Constructs an exception with a given message.
     * 
     * @param message
     *            the exception message
     * @param cause
     *            the cause of this exception in case of chained exceptions
     */
    public UncheckedException(String message, Throwable cause)
    {
        super(message, cause);
    }
    
}
