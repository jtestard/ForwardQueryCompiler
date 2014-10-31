package edu.ucsd.forward.query.physical;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.ast.SetOpExpression.SetOpType;
import edu.ucsd.forward.query.ast.SetQuantifier;
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.SetOperator;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Implementation of the union logical operator.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 */
public class UnionImpl extends AbstractBinaryOperatorImpl<SetOperator>
{
    /**
     * The buffer for the child operator implementations distinct bindings.
     */
    private BindingBuffer m_index;
    
    private boolean       m_left_done;
    
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
    public UnionImpl(SetOperator logical, OperatorImpl left_child, OperatorImpl right_child)
    {
        super(logical, left_child, right_child);
        
        assert (logical.getSetOpType() == SetOpType.UNION);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        m_left_done = false;
        
        if (getOperator().getSetQuantifier() == SetQuantifier.DISTINCT)
        {
            OutputInfo in_info = getOperator().getChildren().get(0).getOutputInfo();
            
            // Conditions to be used by the binding buffer
            List<Term> match_terms = null;
            if (getOperator().getMatchTerms() == null) match_terms = new ArrayList<Term>(getOperator().getVariablesUsed());
            else match_terms = getOperator().getMatchTerms();
            
            m_index = new BindingBufferHash(match_terms, in_info, false);
        }
        super.open();
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        Binding out_binding;
        
        if (!m_left_done)
        {
            // Try getting binding from left child
            out_binding = getLeftChild().next();
            if (getOperator().getSetQuantifier() == SetQuantifier.DISTINCT)
            {
                // Get the next distinct one
                while (out_binding != null)
                {
                    // If there is no matching binding, then output the current one
                    if (!m_index.get(out_binding, false).hasNext())
                    {
                        m_index.add(out_binding);
                        break;
                    }
                    
                    // Else fetch the next binding
                    out_binding = getLeftChild().next();
                }
            }
            if (out_binding != null)
            {
                return out_binding;
            }
            else
            {
                m_left_done = true;
            }
        }
        // Left child exhausted, try right child
        out_binding = getRightChild().next();
        if (getOperator().getSetQuantifier() == SetQuantifier.DISTINCT)
        {
            // Get the next distinct one
            while (out_binding != null)
            {
                // If there is no matching binding, then output the current one
                if (!m_index.get(out_binding, false).hasNext())
                {
                    m_index.add(out_binding);
                    break;
                }
                
                // Else fetch the next binding
                out_binding = getRightChild().next();
            }
        }
        
        return out_binding;
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        if (this.getState() == State.OPEN)
        {
            if (getOperator().getSetQuantifier() == SetQuantifier.DISTINCT)
            {
                m_index.clear();
            }
        }
        
        super.close();
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        UnionImpl copy = new UnionImpl(this.getOperator(), this.getLeftChild().copy(context), this.getRightChild().copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitUnionImpl(this);
    }
    
}
