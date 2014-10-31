/**
 * 
 */
package edu.ucsd.forward.exception;

/**
 * Represents checked exceptions that require a location that will be reported to the user.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class UserException extends CheckedException
{
    private static final long serialVersionUID = 1L;
    
    /**
     * Constructs an exception with a given message, possibly parameterized, for a given location in an input file.
     * 
     * @param message
     *            the exception message
     * @param location
     *            the file location associated with this exception
     * @param params
     *            the parameters that the message is expecting
     */
    public UserException(Messages message, Location location, Object... params)
    {
        super(message, location, null, params);
    }
    
    /**
     * Constructs an exception with a given message, possibly parameterized, for a given location in an input file.
     * 
     * @param message
     *            the exception message
     * @param location
     *            the file location associated with this exception
     * @param cause
     *            the cause of this exception in case of chained exceptions
     * @param params
     *            the parameters that the message is expecting
     */
    public UserException(Messages message, Location location, Throwable cause, Object... params)
    {
        super(message, location, cause, params);
    }
    
}
