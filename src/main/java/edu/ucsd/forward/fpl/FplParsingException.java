/**
 * 
 */
package edu.ucsd.forward.fpl;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.exception.UserException;
import edu.ucsd.forward.exception.ExceptionMessages.QueryParsing;
import edu.ucsd.forward.query.parser.ParseException;
import edu.ucsd.forward.query.parser.Token;

/**
 * Represents an exception thrown during action parsing.
 * 
 * @author Yupeng
 * 
 */
public class FplParsingException extends UserException
{
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(FplParsingException.class);
    
    private static final long   serialVersionUID = 1L;
    
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
    public FplParsingException(QueryParsing message, Location location, Object... params)
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
    public FplParsingException(QueryParsing message, Location location, Throwable cause, Object... params)
    {
        super(message, location, cause, params);
    }
    
    /**
     * Creates an exception based on a parse exception, thrown by JavaCC, and a location.
     * 
     * @param ex
     *            the parse exception thrown by JavaCC.
     * @return an exception.
     */
    public static FplParsingException create(ParseException ex)
    {
        Token token = ex.currentToken;
        Location sub_location = new LocationImpl(Location.UNKNOWN_PATH, token.next.beginLine, token.next.beginColumn,
                                                 token.next.endLine, token.next.endColumn);
        
        int max_size = 0;
        for (int i = 0; i < ex.expectedTokenSequences.length; i++)
        {
            if (max_size < ex.expectedTokenSequences[i].length)
            {
                max_size = ex.expectedTokenSequences[i].length;
            }
        }
        
        String message = "Encountered ";
        Token tok = token.next;
        for (int i = 0; i < max_size; i++)
        {
            if (i != 0) message += " ";
            if (tok.kind == 0)
            {
                message += ex.tokenImage[0];
                break;
            }
            message += ex.tokenImage[tok.kind];
            tok = tok.next;
        }
        
        return new FplParsingException(QueryParsing.QUERY_PARSE_ERROR, sub_location, message);
    }
}
