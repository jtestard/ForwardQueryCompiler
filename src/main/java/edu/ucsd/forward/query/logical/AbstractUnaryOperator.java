package edu.ucsd.forward.query.logical;

import java.util.List;

/**
 * Represents an abstract operator having a single child operator.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public abstract class AbstractUnaryOperator extends AbstractOperator implements UnaryOperator
{
    /**
     * Constructs an instance of the unary operator.
     */
    protected AbstractUnaryOperator()
    {
    }
    
    @Override
    public Operator getChild()
    {
        List<Operator> children = this.getChildren();
        assert (children.size() == 1);
        
        return children.get(0);
    }
    
}
