/**
 * 
 */
package edu.ucsd.forward.query.physical.visitor;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.physical.OperatorImpl;

/**
 * An abstract implementation of the operator implementation visitor interface.
 * 
 * @author Yupeng
 * 
 */
public abstract class AbstractOperatorImplVisitor implements OperatorImplVisitor
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractOperatorImplVisitor.class);
    
    /**
     * Visits the children of an operator implementation.
     * 
     * @param operator_impl
     *            the parent operator implementation.
     */
    protected void visitChildren(OperatorImpl operator_impl)
    {
        for (OperatorImpl child : operator_impl.getChildren())
        {
            child.accept(this);
        }
    }
    
    /**
     * Visits an operator implementation.
     * 
     * @param operator_impl
     *            the operator implementation to be visited.
     */
    protected void visit(OperatorImpl operator_impl)
    {
        operator_impl.accept(this);
    }
}
