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
 * Implementation of the intersect logical operator.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 */
public class IntersectImpl extends AbstractBinaryOperatorImpl<SetOperator>
{
    /**
     * The buffer for the left child operator implementations bindings.
     */
    private BindingBuffer m_left_index;
    
    /**
     * The buffer for the right child operator implementations bindings.
     */
    private BindingBuffer m_right_index;
    
    /**
     * Determines whether the population of the left side is done.
     */
    private boolean       m_population_done;
    
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
    public IntersectImpl(SetOperator logical, OperatorImpl left_child, OperatorImpl right_child)
    {
        super(logical, left_child, right_child);
        
        assert (logical.getSetOpType() == SetOpType.INTERSECT);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        OutputInfo in_info = getOperator().getChildren().get(0).getOutputInfo();
        
        // Conditions to be used by the binding buffer
        List<Term> conditions = new ArrayList<Term>(getOperator().getVariablesUsed());
        
        m_left_index = new BindingBufferHash(conditions, in_info, false);
        if (getOperator().getSetQuantifier() == SetQuantifier.DISTINCT)
        {
            m_right_index = new BindingBufferHash(conditions, in_info, false);
        }
        super.open();
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        // Populate the left buffer
        if (!m_population_done)
        {
            m_left_index.populate(this.getLeftChild());
            m_population_done = true;
        }
        
        // Get the next right value
        Binding right_binding = getRightChild().next();
        while (right_binding != null)
        {
            // Check if the binding matches in the left index
            if (m_left_index.get(right_binding, false).hasNext())
            {
                // Remove the matching left binding
                m_left_index.remove(m_left_index.get(right_binding, false).next());
                
                if (getOperator().getSetQuantifier() == SetQuantifier.DISTINCT)
                {
                    // Check if the current binding is duplicate
                    if (!m_right_index.get(right_binding, false).hasNext())
                    {
                        m_right_index.add(right_binding);
                        break;
                    }
                }
                else
                // Output the binding
                break;
            }
            
            right_binding = getRightChild().next();
        }
        
        return right_binding;
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        if (this.getState() == State.OPEN)
        {
            m_left_index.clear();
            if (getOperator().getSetQuantifier() == SetQuantifier.DISTINCT)
            {
                m_right_index.clear();
            }
        }
        
        super.close();
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        IntersectImpl copy = new IntersectImpl(this.getOperator(), this.getLeftChild().copy(context), this.getRightChild().copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitIntersectImpl(this);
    }
    
}
