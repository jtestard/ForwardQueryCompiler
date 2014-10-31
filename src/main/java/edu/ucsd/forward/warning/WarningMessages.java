/**
 * 
 */
package edu.ucsd.forward.warning;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Messages;

/**
 * A list of warning messages for Warnings.
 * 
 * @author Yupeng
 * 
 */
public final class WarningMessages
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(WarningMessages.class);
    
    /**
     * Hidden constructor.
     */
    private WarningMessages()
    {
        
    }
    
    /**
     * A list of error messages for action compilation.
     */
    public enum ApplicationBuilding implements Messages
    {
        NO_REALM_CONFIG("Found security constraint configurations, but did not find any realm configurations.");
        
        private static final int  MODULE = 71;
        
        private final int         m_code;
        
        private final MessageType m_type;
        
        private final String      m_message;
        
        /**
         * Constructor.
         * 
         * @param message
         *            the message.
         */
        ApplicationBuilding(String message)
        {
            this(MessageType.ERROR, message);
        }
        
        /**
         * Constructor.
         * 
         * @param type
         *            the type of the message.
         * @param message
         *            the message.
         */
        ApplicationBuilding(MessageType type, String message)
        {
            m_code = MODULE;
            m_type = type;
            m_message = message;
        }
        
        @Override
        public int getMessageCode()
        {
            return m_code;
        }
        
        @Override
        public String getMessageId()
        {
            return name();
        }
        
        @Override
        public MessageType getMessageType()
        {
            return m_type;
        }
        
        @Override
        public String getMessage(Object... params)
        {
            return messageFormat(m_message, params);
        }
    }
    
    /**
     * A list of error messages for action compilation.
     */
    public enum ActionCompilation implements Messages
    {
        TYPO_IN_ASSIGNMENT("Typo? Assignment statement uses \":=\" but not \"=\"");
        private static final int  MODULE = 81;
        
        private final int         m_code;
        
        private final MessageType m_type;
        
        private final String      m_message;
        
        /**
         * Constructor.
         * 
         * @param message
         *            the message.
         */
        ActionCompilation(String message)
        {
            this(MessageType.ERROR, message);
        }
        
        /**
         * Constructor.
         * 
         * @param type
         *            the type of the message.
         * @param message
         *            the message.
         */
        ActionCompilation(MessageType type, String message)
        {
            m_code = MODULE;
            m_type = type;
            m_message = message;
        }
        
        @Override
        public int getMessageCode()
        {
            return m_code;
        }
        
        @Override
        public String getMessageId()
        {
            return name();
        }
        
        @Override
        public MessageType getMessageType()
        {
            return m_type;
        }
        
        @Override
        public String getMessage(Object... params)
        {
            return messageFormat(m_message, params);
        }
    }
    
    /**
     * Instantiates a message template given the provided parameters and returns a string.
     * 
     * @param pattern
     *            the message template.
     * @param arguments
     *            the provided parameters.
     * @return an instantiated message template.
     */
    private static String messageFormat(String pattern, Object... arguments)
    {
        String s = pattern;
        int i = 0;
        while (i < arguments.length)
        {
            String delimiter = "{" + i + "}";
            while (s.contains(delimiter))
            {
                s = s.replace(delimiter, String.valueOf(arguments[i]));
            }
            i++;
        }
        return s;
    }
}
