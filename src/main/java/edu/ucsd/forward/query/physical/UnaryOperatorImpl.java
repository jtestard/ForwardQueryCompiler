package edu.ucsd.forward.query.physical;

/**
 * Represents a operator implementation having a single child operator implementation.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface UnaryOperatorImpl extends OperatorImpl
{
    /**
     * Gets the child operator implementation of the unary operator implementation.
     * 
     * @return the child operator implementation.
     */
    public OperatorImpl getChild();
    
}
