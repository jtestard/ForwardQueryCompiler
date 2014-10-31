package edu.ucsd.forward.data.encoding;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * Thrown when a encoded data being parsed is invalid in some way, e.g. it contains a malformed varint or a negative byte length.
 * 
 * @author Aditya Avinash
 */
public class EncodingViolationException extends RuntimeException
{
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(EncodingViolationException.class);
    
    private static final long   serialVersionUID = -1616151763072450476L;
    
    public EncodingViolationException(final String description)
    {
        super(description);
    }
}
