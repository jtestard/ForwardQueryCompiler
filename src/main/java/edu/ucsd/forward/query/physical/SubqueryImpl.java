package edu.ucsd.forward.query.physical;

import edu.ucsd.forward.data.value.LongValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.Subquery;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Implementation of the Subquery logical operator.
 * 
 * @author Romain Vernoux
 */
@SuppressWarnings("serial")
public class SubqueryImpl extends AbstractUnaryOperatorImpl<Subquery>
{
    /**
     * The current input binding.
     */
    private Binding m_in_binding;
    
    /**
     * The counter for the position variable.
     */
    private long    m_order_counter;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical scan operator.
     * @param child
     *            the single child operator implementation.
     */
    public SubqueryImpl(Subquery logical, OperatorImpl child)
    {
        super(logical, child);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        super.open();
        
        m_in_binding = null;
        m_order_counter = 0;
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        m_in_binding = this.getChild().next();
                
        // Fail fast if there is no input binding
        if (m_in_binding == null)
        {
            return null;
        }
                
        // Normal form assumption: the operators below can only output tuples with a single attribute.
        assert (m_in_binding.getValues().size() == 1);
        
        // Create a new binding
        Binding binding = new Binding();
        
        binding.addValue(m_in_binding.getValue(0));
        
        if(this.getOperator().getOrderVariable() != null)
        {
            binding.addValue(new BindingValue(new LongValue(m_order_counter++), false));
        }
        
        return binding;
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        if (this.getState() == State.OPEN)
        {
            m_in_binding = null;
        }
        
        super.close();
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        SubqueryImpl copy = new SubqueryImpl(getOperator(), getChild().copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitSubqueryImpl(this);
    }
    
}
