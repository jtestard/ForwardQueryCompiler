/**
 * 
 */
package edu.ucsd.forward.query.ast.literal;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * The boolean literal.
 * 
 * @author Yupeng
 * 
 */
public class BooleanLiteral extends AbstractScalarLiteral
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(BooleanLiteral.class);
    
    /**
     * The constructor with the literal.
     * 
     * @param literal
     *            the boolean literal.
     * @param location
     *            a location.
     */
    public BooleanLiteral(boolean literal, Location location)
    {
        super(Boolean.toString(literal), location);
        
        this.setScalarValue(new BooleanValue(literal));
    }
    
    /**
     * The constructor with the value.
     * 
     * @param value
     *            the boolean value.
     * @param location
     *            a location.
     */
    public BooleanLiteral(BooleanValue value, Location location)
    {
        super(value, location);
        
        this.setLiteral(value.getObject().toString());
    }
    
    @Override
    public BooleanValue getScalarValue()
    {
        return (BooleanValue) super.getScalarValue();
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitBooleanLiteral(this);
    }
    
    @Override
    public AstNode copy()
    {
        BooleanLiteral copy = new BooleanLiteral(this.getScalarValue().getObject().booleanValue(), this.getLocation());
        
        super.copy(copy);
        
        return copy;
    }
    
}
