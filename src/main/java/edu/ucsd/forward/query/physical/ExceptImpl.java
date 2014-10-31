package edu.ucsd.forward.query.physical;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.ast.SetQuantifier;
import edu.ucsd.forward.query.ast.SetOpExpression.SetOpType;
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.SetOperator;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Implementation of the except logical operator.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 */
public class ExceptImpl extends AbstractBinaryOperatorImpl<SetOperator>
{
    
    /**
     * The buffer for the left child operator implementations bindings.
     */
    private BindingBuffer m_left_index;
    
    /**
     * The buffer for the right child operator implementations bindings.
     */
    private BindingBuffer m_right_index;
    
    private boolean       m_init = false;
    
    /**
     * Constructs an instance of the operator implementation.
     * 
     * @param logical
     *            a logical join operator.
     * @param left_child
     *            the left child operator implementation.
     * @param right_child
     *            the right child operator implementation.
     */
    public ExceptImpl(SetOperator logical, OperatorImpl left_child, OperatorImpl right_child)
    {
        super(logical, left_child, right_child);
        
        assert (logical.getSetOpType() == SetOpType.EXCEPT);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        m_init = false;
        
        OutputInfo in_info = getOperator().getChildren().get(0).getOutputInfo();
        
        // Conditions to be used by the binding buffer
        List<Term> conditions = new ArrayList<Term>(getOperator().getVariablesUsed());
        
        if (getOperator().getSetQuantifier() == SetQuantifier.DISTINCT)
        {
            m_left_index = new BindingBufferHash(conditions, in_info, false);
        }
        m_right_index = new BindingBufferHash(conditions, in_info, false);
        super.open();
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        // Populate the right buffer
        if (!m_init)
        {
            m_right_index.populate(this.getRightChild());
            m_init = true;
        }
        
        // Get the next left value
        Binding out_binding = getLeftChild().next();
        while (out_binding != null)
        {
            if (getOperator().getSetQuantifier() == SetQuantifier.DISTINCT)
            {
                // Check if the current binding is duplicate
                if (!m_left_index.get(out_binding, false).hasNext())
                {
                    // Record it
                    m_left_index.add(out_binding);
                }
                else
                {
                    out_binding = getLeftChild().next();
                    // Fetch the next
                    continue;
                }
            }
            
            // Check if the binding matches in the right index
            if (m_right_index.get(out_binding, false).hasNext())
            {
                // Remove the matching right binding
                m_right_index.remove(m_right_index.get(out_binding, false).next());
            }
            else
            // Output the binding
            break;
            
            out_binding = getLeftChild().next();
        }
        
        return out_binding;
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        if (this.getState() == State.OPEN)
        {
            m_right_index.clear();
            if (getOperator().getSetQuantifier() == SetQuantifier.DISTINCT)
            {
                m_left_index.clear();
            }
            m_init = false;
        }
        
        super.close();
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        ExceptImpl copy = new ExceptImpl(this.getOperator(), this.getLeftChild().copy(context), this.getRightChild().copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitExceptImpl(this);
    }
    
}
