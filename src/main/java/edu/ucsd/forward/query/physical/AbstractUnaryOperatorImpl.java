package edu.ucsd.forward.query.physical;

import edu.ucsd.forward.query.logical.Operator;

/**
 * Represents an abstract operator implementation having a single child operator implementation.
 * 
 * @param <O>
 *            the class of the logical operator being implemented.
 * 
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractUnaryOperatorImpl<O extends Operator> extends AbstractOperatorImpl<O> implements UnaryOperatorImpl
{
    /**
     * Constructs an abstract unary operator implementation for a given logical operator.
     * 
     * @param logical
     *            a logical operator.
     */
    protected AbstractUnaryOperatorImpl(O logical)
    {
        super(logical);
    }
    
    /**
     * Constructs an abstract unary operator implementation for a given logical operator and a child operator implementation.
     * 
     * @param logical
     *            a logical operator.
     * @param child
     *            the child operator implementation.
     */
    protected AbstractUnaryOperatorImpl(O logical, OperatorImpl child)
    {
        this(logical);
        
        assert (child != null);
        
        this.addChild(child);
    }
    
    @Override
    public OperatorImpl getChild()
    {
        return this.getChildren().get(0);
    }
    
}
