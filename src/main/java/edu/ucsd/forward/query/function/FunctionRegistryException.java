/**
 * 
 */
package edu.ucsd.forward.query.function;

import edu.ucsd.forward.exception.CheckedException;
import edu.ucsd.forward.exception.ExceptionMessages;

/**
 * Represents an exception related to function registration.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class FunctionRegistryException extends CheckedException
{
    private static final long serialVersionUID = 1L;
    
    /**
     * Constructs an exception with a given message, possibly parameterized.
     * 
     * @param message
     *            the exception message
     * @param params
     *            the parameters that the message is expecting
     */
    public FunctionRegistryException(ExceptionMessages.Function message, Object... params)
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
    public FunctionRegistryException(ExceptionMessages.Function message, Throwable cause, Object... params)
    {
        super(message, cause, params);
    }
    
}
