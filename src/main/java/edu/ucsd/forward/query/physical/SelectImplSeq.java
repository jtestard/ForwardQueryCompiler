package edu.ucsd.forward.query.physical;

import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.Select;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Represents an implementation of the one-pass tuple-at-a-time (sequential) algorithm for the selection logical operator.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class SelectImplSeq extends AbstractUnaryOperatorImpl<Select>
{
    /**
     * The selection condition.
     */
    private Term m_condition;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical selection operator.
     * @param child
     *            the single child operator implementation.
     */
    public SelectImplSeq(Select logical, OperatorImpl child)
    {
        super(logical, child);
        
        m_condition = logical.getMergedCondition();
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        Binding in_binding = null;
        Value result;
        
        while (true)
        {
            // Get the next binding from the child operator implementation
            in_binding = this.getChild().next();
            
            if (in_binding == null) break;
            
            result = TermEvaluator.evaluate(m_condition, in_binding).getValue();
            
            if (result instanceof BooleanValue && ((BooleanValue) result).getObject()) break;
        }
        
        return in_binding;
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        SelectImplSeq copy = new SelectImplSeq(getOperator(), getChild().copy(context));
        copy.m_condition = m_condition;
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitSelectImplSeq(this);
    }
    
}
