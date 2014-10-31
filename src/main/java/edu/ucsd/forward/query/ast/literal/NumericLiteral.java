/**
 * 
 */
package edu.ucsd.forward.query.ast.literal;

import java.math.BigDecimal;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.value.DecimalValue;
import edu.ucsd.forward.data.value.DoubleValue;
import edu.ucsd.forward.data.value.FloatValue;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.LongValue;
import edu.ucsd.forward.data.value.NumericValue;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * Represents a numeric literal.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class NumericLiteral extends AbstractScalarLiteral
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(NumericLiteral.class);
    
    /**
     * Constructor with a literal.
     * 
     * @param literal
     *            the numeric literal.
     * @param location
     *            a location.
     */
    public NumericLiteral(String literal, Location location)
    {
        super(literal, location);
        
        assert (literal != null);
        
        NumericValue<?> coerced_value = coerse(literal);
        this.setScalarValue(coerced_value);
    }
    
    /**
     * Constructor with a value.
     * 
     * @param value
     *            the numeric value.
     * @param location
     *            a location.
     */
    public NumericLiteral(NumericValue<?> value, Location location)
    {
        super(value, location);
        
        this.setLiteral(value.getObject().toString());
    }
    
    @Override
    public NumericValue<?> getScalarValue()
    {
        return (NumericValue<?>) super.getScalarValue();
    }
    
    /**
     * This method coerces the input value into a low-precision type while preserving the initial precision.
     * 
     * @param literal
     *            the literal to coerce.
     * @return a coerced value.
     */
    private NumericValue<?> coerse(String literal)
    {
        BigDecimal decimal_obj = new BigDecimal(literal);
        
        int int_value = decimal_obj.intValue();
        if (decimal_obj.compareTo(new BigDecimal(int_value)) == 0)
        {
            return new IntegerValue(int_value);
        }
        
        long long_value = decimal_obj.longValue();
        if (decimal_obj.compareTo(new BigDecimal(long_value)) == 0)
        {
            return new LongValue(long_value);
        }
        
        float float_value = decimal_obj.floatValue();
        if (decimal_obj.compareTo(new BigDecimal(float_value)) == 0)
        {
            return new FloatValue(float_value);
        }
        
        double double_value = decimal_obj.doubleValue();
        if (decimal_obj.compareTo(new BigDecimal(double_value)) == 0)
        {
            return new DoubleValue(double_value);
        }
        
        return new DecimalValue(decimal_obj);
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitNumericLiteral(this);
    }
    
    @Override
    public AstNode copy()
    {
        NumericLiteral copy = new NumericLiteral(this.getLiteral(), this.getLocation());
        
        super.copy(copy);
        
        return copy;
    }
    
}
