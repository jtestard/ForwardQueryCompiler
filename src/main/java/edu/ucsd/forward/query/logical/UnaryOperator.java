package edu.ucsd.forward.query.logical;

/**
 * Represents an operator having a single child operator.
 * 
 * @author Michalis Petropoulos
 */
public interface UnaryOperator extends Operator
{
    /**
     * Gets the child operator of the unary operator.
     * 
     * @return the child operator.
     */
    public Operator getChild();
    
}
