/**
 * 
 */
package edu.ucsd.forward.data.source;

import edu.ucsd.forward.exception.CheckedException;
import edu.ucsd.forward.exception.ExceptionMessages;

/**
 * Represents an exception thrown while accessing a data source.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class DataSourceException extends CheckedException
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
    public DataSourceException(ExceptionMessages.DataSource message, Object... params)
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
    public DataSourceException(ExceptionMessages.DataSource message, Throwable cause, Object... params)
    {
        super(message, cause, params);
    }
    
}
