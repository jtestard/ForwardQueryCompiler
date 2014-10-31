/**
 * 
 */
package edu.ucsd.forward.exception;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * Represents checked exceptions. Each checked exception is associated with a message in ExceptionMessages, which contains display
 * templates for checked exceptions, and the exceptions provide the actual parameters for the templates.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class CheckedException extends Exception
{
    private static final long   serialVersionUID = 1L;
    
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(CheckedException.class);
    
    public static final String  SPACER           = ": ";
    
    private final Messages      m_message;
    
    private final Object[]      m_params;
    
    private final Location      m_location;
    
    /**
     * Constructs an exception with a given message, possibly parameterized.
     * 
     * @param message
     *            the exception message
     * @param params
     *            the parameters that the message is expecting
     */
    public CheckedException(Messages message, Object... params)
    {
        this(message, null, null, params);
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
    public CheckedException(Messages message, Location location, Object... params)
    {
        this(message, location, null, params);
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
    public CheckedException(Messages message, Throwable cause, Object... params)
    {
        this(message, null, cause, params);
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
    public CheckedException(Messages message, Location location, Throwable cause, Object... params)
    {
        super(message.getMessage(params), cause);
        m_message = message;
        m_params = params;
        m_location = location;
    }
    
    /**
     * Gets the parameterized message associated with this exception.
     * 
     * @return the parameterized message.
     */
    public Messages getParameterizedMessage()
    {
        return m_message;
    }
    
    /**
     * Gets the message parameters passed to this exception.
     * 
     * @return the message parameters
     */
    public Object[] getMessageParams()
    {
        return m_params;
    }
    
    /**
     * Gets the location associated with this exception, if there is one. Returns null otherwise.
     * 
     * @return the location.
     */
    public Location getLocation()
    {
        return m_location;
    }
    
    /**
     * Returns the message of this exception, and recursively the message of its cause, in a single line.
     * 
     * @return a single line message.
     */
    public String getSingleLineMessage()
    {
        String str = this.getMessage();
        
        if (this.getCause() != null)
        {
            if (this.getCause() instanceof CheckedException)
            {
                str += SPACER + ((CheckedException) this.getCause()).getSingleLineMessage();
            }
            else
            {
                str += SPACER + this.getCause().getMessage();
            }
        }
        
        return str;
    }
    
    /**
     * Returns the message of this exception, and recursively the message of its cause, in a single line, including location
     * information.
     * 
     * @return a single line message.
     */
    public String getSingleLineMessageWithLocation()
    {
        return getSingleLineMessageWithLocation(true);
    }
    
    /**
     * Returns the message of this exception, and recursively the message of its cause, in a single line, including location
     * information.
     * 
     * @param include_top_level_location
     *            whether to include the location information for the top level message
     * 
     * @return a single line message.
     */
    public String getSingleLineMessageWithLocation(boolean include_top_level_location)
    {
        String str = this.getMessage();
        
        if (m_location != null && include_top_level_location)
        {
            str += " " + m_location.toString();
        }
        
        if (this.getCause() != null)
        {
            if (this.getCause() instanceof CheckedException)
            {
                str += SPACER + ((CheckedException) this.getCause()).getSingleLineMessageWithLocation();
            }
            else
            {
                str += SPACER + this.getCause().getMessage();
            }
        }
        
        return str;
    }
    
    @Override
    public String toString()
    {
        return getSingleLineMessageWithLocation();
    }
}
