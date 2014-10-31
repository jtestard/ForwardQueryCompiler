/**
 * 
 */
package edu.ucsd.forward.query;

import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.ExceptionMessages.QueryCompilation;
import edu.ucsd.forward.fpl.FplCompilationException;

/**
 * Represents an exception thrown during query compilation.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 */
public class QueryCompilationException extends FplCompilationException
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
    public QueryCompilationException(QueryCompilation message, Location location, Object... params)
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
    public QueryCompilationException(QueryCompilation message, Location location, Throwable cause, Object... params)
    {
        super(message, location, cause, params);
    }
    
}
