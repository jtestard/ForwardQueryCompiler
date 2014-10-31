/**
 * 
 */
package edu.ucsd.forward.fpl;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.UserException;
import edu.ucsd.forward.exception.ExceptionMessages.ActionCompilation;
import edu.ucsd.forward.exception.ExceptionMessages.QueryCompilation;

/**
 * Represents an exception thrown during action compilation.
 * 
 * @author Yupeng
 * 
 */
public class FplCompilationException extends UserException
{
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(FplCompilationException.class);
    
    private static final long   serialVersionUID = 1L;
    
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
    public FplCompilationException(ActionCompilation message, Location location, Object... params)
    {
        super(message, location, params);
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
    public FplCompilationException(ActionCompilation message, Location location, Throwable cause, Object... params)
    {
        super(message, location, cause, params);
    }
    
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
    public FplCompilationException(QueryCompilation message, Location location, Object... params)
    {
        super(message, location, params);
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
    public FplCompilationException(QueryCompilation message, Location location, Throwable cause, Object... params)
    {
        super(message, location, cause, params);
    }
    
}
