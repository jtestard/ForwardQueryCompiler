package edu.ucsd.forward.query.logical;

/**
 * Represents an operator having two child operators.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface BinaryOperator extends Operator
{
    /**
     * Gets the left child operator of the binary operator.
     * 
     * @return the left child operator.
     */
    public Operator getLeftChild();
    
    /**
     * Gets the right child operator of the binary operator.
     * 
     * @return the right child operator.
     */
    public Operator getRightChild();
    
}
