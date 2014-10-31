/**
 * 
 */
package edu.ucsd.forward.data.source;

import edu.ucsd.forward.exception.ExceptionMessages;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.UserException;

/**
 * Represents an exception thrown while configuring a data source.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class DataSourceConfigException extends UserException
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
    public DataSourceConfigException(ExceptionMessages.DataSource message, Location location, Object... params)
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
    public DataSourceConfigException(ExceptionMessages.DataSource message, Location location, Throwable cause, Object... params)
    {
        super(message, location, cause, params);
    }
    
}
