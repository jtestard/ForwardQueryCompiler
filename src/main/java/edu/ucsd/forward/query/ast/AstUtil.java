/**
 * 
 */
package edu.ucsd.forward.query.ast;

import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.NumericValue;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.ast.literal.BooleanLiteral;
import edu.ucsd.forward.query.ast.literal.Literal;
import edu.ucsd.forward.query.ast.literal.NullLiteral;
import edu.ucsd.forward.query.ast.literal.NumericLiteral;
import edu.ucsd.forward.query.ast.literal.StringLiteral;

/**
 * Utility methods for AST nodes.
 * 
 * @author Romain Vernoux
 * 
 */
public final class AstUtil
{
    /**
     * Hidden constructor.
     */
    private AstUtil()
    {
        
    }
    
    /**
     * Translates a value into the corresponding value expression.
     * 
     * @param value
     *            the value to translate.
     * @return a corresponding value expression.
     */
    public static ValueExpression translateValue(Value value)
    {
        if (value instanceof NullValue)
        {
            return new NullLiteral(new LocationImpl(Location.UNKNOWN_PATH));
        }
        else if (value instanceof ScalarValue)
        {
            return translateConstant(value);
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }
    
    /**
     * Translates the given scalar or null value into a literal.
     * 
     * @param value
     *            the given scalar or null value.
     * @return a literal.
     */
    public static Literal translateConstant(Value value)
    {
        if (value instanceof BooleanValue)
        {
            return new BooleanLiteral((BooleanValue) value, new LocationImpl(Location.UNKNOWN_PATH));
        }
        else if (value instanceof NumericValue<?>)
        {
            return new NumericLiteral((NumericValue<?>) value, new LocationImpl(Location.UNKNOWN_PATH));
        }
        else if (value instanceof StringValue)
        {
            return new StringLiteral((StringValue) value, new LocationImpl(Location.UNKNOWN_PATH));
        }
        else if (value instanceof NullValue)
        {
            return new NullLiteral(new LocationImpl(Location.UNKNOWN_PATH));
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }
}
