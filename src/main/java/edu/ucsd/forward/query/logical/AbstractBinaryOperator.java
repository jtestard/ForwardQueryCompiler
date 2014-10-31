package edu.ucsd.forward.query.logical;

import java.util.List;

/**
 * Represents an abstract operator having two child operators.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public abstract class AbstractBinaryOperator extends AbstractOperator implements BinaryOperator
{
    /**
     * Constructs an instance of the binary operator.
     * 
     * @param left_child
     *            the left child operator.
     * @param right_child
     *            the right child operator.
     */
    protected AbstractBinaryOperator(Operator left_child, Operator right_child)
    {
        assert (left_child != null);
        assert (right_child != null);
        
        this.addChild(left_child);
        this.addChild(right_child);
    }
    
    /**
     * Default constructor.
     */
    @SuppressWarnings("unused")
    private AbstractBinaryOperator()
    {
        
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.query.logical.BinaryOperator#getLeftChild()
     */
    @Override
    public Operator getLeftChild()
    {
        List<Operator> children = this.getChildren();
        assert (children.size() == 2);
        
        return children.get(0);
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.query.logical.BinaryOperator#getRightChild()
     */
    @Override
    public Operator getRightChild()
    {
        List<Operator> children = this.getChildren();
        assert (children.size() == 2);
        
        return children.get(1);
    }
    
}
