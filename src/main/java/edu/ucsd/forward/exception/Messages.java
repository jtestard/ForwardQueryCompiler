package edu.ucsd.forward.exception;

/**
 * Represents messages that will be reported to the user. Each message consists of a code, an identifier, a type, and a
 * parameterized template for a description.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface Messages
{
    /**
     * Returns the unique code corresponding to the message. The message code is unique across all messages used in the framework.
     * 
     * @return the unique code corresponding to the message.
     */
    public int getMessageCode();
    
    /**
     * Returns the string identifier corresponding to the message. The message identifier is unique within a given enumeration, but
     * not necessarily across all messages used by the framework.
     * 
     * @return the string identifier corresponding to the message.
     */
    public String getMessageId();
    
    /**
     * The types of supported messages.
     * 
     * @author Michalis Petropoulos
     */
    public enum MessageType
    {
        ERROR, WARNING
    };
    
    /**
     * Returns the type of the message.
     * 
     * @return the string identifier corresponding to the message.
     */
    public MessageType getMessageType();
    
    /**
     * Returns the message that might be parameterized.
     * 
     * @param params
     *            the parameters expected by the message.
     * @return the message.
     */
    public String getMessage(Object... params);
}
