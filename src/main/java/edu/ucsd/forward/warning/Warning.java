/**
 * 
 */
package edu.ucsd.forward.warning;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.Messages;

/**
 * Represents a warning during application compilation. Each warning is associated with a message in WarningMessages, which contains
 * display templates for caught warnings, and the warnings provide the actual parameters for the templates.
 * 
 * @author Yupeng
 * 
 */
public class Warning
{
    @SuppressWarnings("unused")
    private static final Logger log    = Logger.getLogger(Warning.class);
    
    public static final String  SPACER = ": ";
    
    private final Messages      m_message;
    
    private final Object[]      m_params;
    
    private final Location      m_location;
    
    /**
     * Constructs a warning with a given message, possibly parameterized.
     * 
     * @param message
     *            the exception message
     * @param params
     *            the parameters that the message is expecting
     */
    public Warning(Messages message, Object... params)
    {
        this(message, null, params);
    }
    
    /**
     * Constructs a warning with a given message, possibly parameterized, for a given location in an input file.
     * 
     * @param message
     *            the exception message
     * @param location
     *            the file location associated with this exception
     * @param params
     *            the parameters that the message is expecting
     */
    public Warning(Messages message, Location location, Object... params)
    {
        m_message = message;
        m_params = params;
        m_location = location;
    }
    
    /**
     * Gets the parameterized message associated with this warning.
     * 
     * @return the parameterized message.
     */
    public Messages getParameterizedMessage()
    {
        return m_message;
    }
    
    /**
     * Gets the message parameters passed to this warning.
     * 
     * @return the message parameters
     */
    public Object[] getMessageParams()
    {
        return m_params;
    }
    
    /**
     * Gets the location associated with this warning, if there is one. Returns null otherwise.
     * 
     * @return the location.
     */
    public Location getLocation()
    {
        return m_location;
    }
    
    /**
     * /** Returns the message of this warning, including location information.
     * 
     * @return a message with location.
     */
    public String getMessageWithLocation()
    {
        return getMessageWithLocation(true);
    }
    
    /**
     * Gets the warning message.
     * 
     * @return the warning message.
     */
    public String getMessage()
    {
        return m_message.getMessage(m_params);
    }
    
    /**
     * Returns the message of this warning, including location information.
     * 
     * @param include_top_level_location
     *            whether to include the location information for the message
     * 
     * @return a single line message.
     */
    public String getMessageWithLocation(boolean include_top_level_location)
    {
        String str = this.getMessage();
        
        if (m_location != null && include_top_level_location)
        {
            str += " " + m_location.toString();
        }
        
        return str;
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see java.lang.Throwable#toString()
     */
    @Override
    public String toString()
    {
        return getMessageWithLocation();
    }
}
