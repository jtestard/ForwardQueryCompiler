package edu.ucsd.forward.data.encoding;

import java.io.IOException;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * Thrown when a encoded data being parsed is invalid in some way, e.g. it contains a malformed varint or a negative byte length.
 * 
 * @author Aditya Avinash
 */
public class DataEncodingException extends IOException
{
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(DataEncodingException.class);
    
    private static final long   serialVersionUID = -1616151763072450476L;
    
    public DataEncodingException(final String description)
    {
        super(description);
    }
    
    static DataEncodingException truncatedMessage()
    {
        return new DataEncodingException("While parsing an encoded message, the input ended unexpectedly "
                + "in the middle of a field.  This could mean either than the "
                + "input has been truncated or that an embedded message " + "misreported its own length.");
    }
    
    static DataEncodingException negativeSize()
    {
        return new DataEncodingException("CodedInputStream encountered an embedded string or message "
                + "which claimed to have negative size.");
    }
    
    static DataEncodingException malformedVarint()
    {
        return new DataEncodingException("Block encountered a malformed varint.");
    }
    
}
