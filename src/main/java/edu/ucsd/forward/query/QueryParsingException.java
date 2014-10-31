/**
 * 
 */
package edu.ucsd.forward.query;

import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.exception.ExceptionMessages.QueryParsing;
import edu.ucsd.forward.fpl.FplParsingException;
import edu.ucsd.forward.query.parser.ParseException;
import edu.ucsd.forward.query.parser.Token;
import edu.ucsd.forward.query.parser.TokenMgrError;

/**
 * Represents an exception thrown during query parsing.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class QueryParsingException extends FplParsingException
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
    public QueryParsingException(QueryParsing message, Location location, Object... params)
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
    public QueryParsingException(QueryParsing message, Location location, Throwable cause, Object... params)
    {
        super(message, location, cause, params);
    }
    
    /**
     * Creates an exception based on a parse exception, thrown by JavaCC, and a location.
     * 
     * @param ex
     *            the parse exception thrown by JavaCC.
     * @param path
     *            the path in which the exception occurred.
     * @return an exception.
     */
    public static QueryParsingException create(ParseException ex, String path)
    {
        Token token = ex.currentToken;
        Location sub_location = new LocationImpl(path, token.next.beginLine, token.next.beginColumn, token.next.endLine,
                                                 token.next.endColumn);
        
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
        
        return new QueryParsingException(QueryParsing.QUERY_PARSE_ERROR, sub_location, message);
    }
    
    /**
     * Creates an exception based on a token manager error, thrown by JavaCC, and a location.
     * 
     * @param error
     *            the token manager error thrown by JavaCC.
     * @param path
     *            the path in which the error occurred.
     * @return an exception.
     */
    public static QueryParsingException create(TokenMgrError error, String path)
    {
        Location sub_location = new LocationImpl(path);
        
        return new QueryParsingException(QueryParsing.QUERY_PARSE_ERROR, sub_location, error.getMessage());
    }
    
}
