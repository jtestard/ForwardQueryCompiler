/**
 * 
 */
package edu.ucsd.forward.query.logical.visitors;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.forward.query.logical.Operator;

/**
 * An abstract implementation of the operator visitor interface.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 */
public abstract class AbstractOperatorVisitor implements OperatorVisitor
{
    /**
     * Visits the children of an operator.
     * 
     * @param operator
     *            the parent operator.
     */
    protected void visitChildren(Operator operator)
    {
        List<Operator> children = new ArrayList<Operator>(operator.getChildren());
        for (Operator child : children)
        {
            child.accept(this);
        }
    }
}
