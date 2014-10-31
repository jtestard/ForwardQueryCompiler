package edu.ucsd.forward.query.physical;

/**
 * Represents an operator implementation having two child operator implementations.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface BinaryOperatorImpl extends OperatorImpl
{
    /**
     * Gets the left child operator implementation of the binary operator implementation.
     * 
     * @return the left child operator implementation.
     */
    public OperatorImpl getLeftChild();
    
    /**
     * Gets the right child operator implementation of the binary operator implementation.
     * 
     * @return the right child operator implementation.
     */
    public OperatorImpl getRightChild();
    
}
