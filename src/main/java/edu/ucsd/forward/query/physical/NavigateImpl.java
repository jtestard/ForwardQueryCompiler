package edu.ucsd.forward.query.physical;

import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.Navigate;
import edu.ucsd.forward.query.logical.plan.LogicalPlanUtil;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Implementation of the navigate logical operator.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class NavigateImpl extends AbstractUnaryOperatorImpl<Navigate>
{
    /**
     * Whether the output value is independent of the input binding, that is, the navigate term does not have any relative
     * variables.
     */
    private boolean m_invariant;
    
    /**
     * The value in case it is independent of the input binding.
     */
    private Value   m_invariant_value;
    
    /**
     * The current input binding.
     */
    private Binding m_in_binding;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical navigate operator.
     * @param child
     *            the single child operator implementation.
     */
    public NavigateImpl(Navigate logical, OperatorImpl child)
    {
        super(logical, child);
        
        m_invariant = LogicalPlanUtil.getRelativeVariables(this.getOperator().getTerm().getVariablesUsed()).isEmpty();
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        super.open();
        
        m_invariant_value = null;
        m_in_binding = null;
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        // Get the next input binding
        m_in_binding = this.getChild().next();
        
        // Fail fast if there is no input binding
        if (m_in_binding == null)
        {
            return null;
        }
        
        Value next_value = null;
        
        if (m_invariant)
        {
            if (m_invariant_value == null)
            {
                // Evaluate the navigate term
                Term term = this.getOperator().getTerm();
                m_invariant_value = TermEvaluator.evaluate(term, m_in_binding).getValue();
            }
            
            next_value = m_invariant_value;
        }
        else
        {
            // Reset the cloned flags since binding values are used in multiple bindings
            m_in_binding.resetCloned();
            
            // Evaluate the navigate term
            Term term = this.getOperator().getTerm();
            next_value = TermEvaluator.evaluate(term, m_in_binding).getValue();
        }
        
        // Create a new binding
        Binding binding = new Binding();
        
        // Copy the values from the incoming binding
        for (BindingValue b_value : m_in_binding.getValues())
        {
            b_value.resetCloned();
            binding.addValue(b_value);
        }
        
        // Append the tuple value
        binding.addValue(new BindingValue(next_value, false));
        
        return binding;
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        if (this.getState() == State.OPEN)
        {
            m_invariant_value = null;
            m_in_binding = null;
        }
        
        super.close();
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        NavigateImpl copy = new NavigateImpl(getOperator(), getChild().copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitNavigateImpl(this);
    }
    
}
