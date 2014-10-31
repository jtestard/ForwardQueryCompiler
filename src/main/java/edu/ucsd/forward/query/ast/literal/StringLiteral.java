/**
 * 
 */
package edu.ucsd.forward.query.ast.literal;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * Represents a string value literal.
 * 
 * @author Yupeng
 * 
 */
public class StringLiteral extends AbstractScalarLiteral
{
    @SuppressWarnings("unused")
    private static final Logger log   = Logger.getLogger(StringLiteral.class);
    
    public static final String  QUOTE = "'";
    
    /**
     * Constructs a string literal given a quoted literal.
     * 
     * @param literal
     *            a quoted literal.
     * @param location
     *            a location.
     */
    public StringLiteral(String literal, Location location)
    {
        super(literal, location);
        
        // Get rid of the first and last single quotes, as well as the double (escaped) quotes
        String trimmed_literal = literal.substring(1, literal.length() - 1);
        trimmed_literal = trimmed_literal.replaceAll(QUOTE + QUOTE, QUOTE);
        
        this.setScalarValue(new StringValue(trimmed_literal));
    }
    
    /**
     * Constructs a string literal given a string value.
     * 
     * @param value
     *            a string value.
     * @param location
     *            a location.
     */
    public StringLiteral(StringValue value, Location location)
    {
        super(value, location);
        
        this.setLiteral(StringLiteral.escape(value));
    }
    
    /**
     * Escapes a StringValue to a SQL string.
     * 
     * @param value
     *            a string value.
     * @return an escaped SQL string.
     */
    public static String escape(StringValue value)
    {
        String escaped = new String(value.getObject());
        // Escape the quotes in the literal
        escaped = escaped.replaceAll(QUOTE, QUOTE + QUOTE);
        // Add the first and last single quotes
        escaped = QUOTE + escaped + QUOTE;
        
        return escaped;
    }
    
    /**
     * Escapes a string to a SQL string.
     * 
     * @param value
     *            a string value.
     * @return an escaped SQL string.
     */
    public static String escape(String value)
    {
        String escaped = value;
        // Escape the quotes in the literal
        escaped = escaped.replaceAll(QUOTE, QUOTE + QUOTE);
        // Add the first and last single quotes
        escaped = QUOTE + escaped + QUOTE;
        
        return escaped;
    }
    
    @Override
    public StringValue getScalarValue()
    {
        return (StringValue) super.getScalarValue();
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitStringLiteral(this);
    }
    
    @Override
    public AstNode copy()
    {
        StringLiteral copy = new StringLiteral(this.getScalarValue(), this.getLocation());
        
        super.copy(copy);
        
        return copy;
    }
    
}
