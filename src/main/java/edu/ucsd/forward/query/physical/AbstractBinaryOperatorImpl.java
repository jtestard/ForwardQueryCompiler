package edu.ucsd.forward.query.physical;

import java.util.List;

import edu.ucsd.forward.query.logical.Operator;

/**
 * Represents an abstract operator implementation having two child operator implementations.
 * 
 * @param <O>
 *            the class of the logical operator being implemented.
 * 
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractBinaryOperatorImpl<O extends Operator> extends AbstractOperatorImpl<O> implements BinaryOperatorImpl
{
    /**
     * Constructs an instance of the binary operator implementation for a given logical operator and child operator implementations.
     * 
     * @param logical
     *            a logical operator.
     * @param left_child
     *            the left child operator implementation.
     * @param right_child
     *            the right child operator implementation.
     */
    protected AbstractBinaryOperatorImpl(O logical, OperatorImpl left_child, OperatorImpl right_child)
    {
        super(logical);
        
        assert (left_child != null);
        assert (right_child != null);
        
        this.addChild(left_child);
        this.addChild(right_child);
    }
    
    /**
     * Constructs an instance of the binary operator implementation for a given logical operator and child operator implementations.
     * 
     * @param logical
     *            a logical operator.
     */
    protected AbstractBinaryOperatorImpl(O logical)
    {
        super(logical);
    }
    
    @Override
    public OperatorImpl getLeftChild()
    {
        List<OperatorImpl> children = this.getChildren();
        
        return children.get(0);
    }
    
    @Override
    public OperatorImpl getRightChild()
    {
        List<OperatorImpl> children = this.getChildren();
        assert (children.size() == 2);
        
        return children.get(1);
    }
    
}
