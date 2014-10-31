/**
 * 
 */
package edu.ucsd.forward.xml;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.CheckedException;
import edu.ucsd.forward.exception.ExceptionMessages.Other;

/**
 * An exception that occurs when parsing an XML node into a value or a type.
 * 
 * @author Yupeng
 * 
 */
public class XmlParserException extends CheckedException
{
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(XmlParserException.class);
    
    private static final long   serialVersionUID = 1L;
    
    /**
     * Constructs the exception.
     * 
     * @param message
     *            the message.
     * @param e
     *            the chained exception.
     */
    public
    XmlParserException(String message, Exception e)
    {
        super(Other.XML_PARSER, e, message);
    }
    
    /**
     * Constructs the exception.
     * 
     * @param message
     *            the message.
     */
    public XmlParserException(String message)
    {
        super(Other.XML_PARSER, message);
    }
}
