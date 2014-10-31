package edu.ucsd.forward.query.physical;

import java.util.LinkedList;

import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.LongValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Implementation of the Scan logical operator.
 * 
 * @author Michalis Petropoulos
 * @author Romain Vernoux
 */
@SuppressWarnings("serial")
public class ScanImpl extends AbstractUnaryOperatorImpl<Scan>
{
    /**
     * The tuple value iterator.
     */
    private LinkedList<Value> m_element_list;
    
    /**
     * The current input binding.
     */
    private Binding           m_in_binding;
    
    /**
     * The counter for the position variable.
     */
    private long              m_order_counter;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical scan operator.
     * @param child
     *            the single child operator implementation.
     */
    public ScanImpl(Scan logical, OperatorImpl child)
    {
        super(logical, child);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        super.open();
        
        m_in_binding = null;
        m_element_list = new LinkedList<Value>();
        m_order_counter = 0;
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        // Get the next value
        Value next_value = m_element_list.poll();
        
        if (next_value == null)
        {
            while (true)
            {
                m_in_binding = this.getChild().next();
                
                // Fail fast if there is no input binding
                if (m_in_binding == null)
                {
                    return null;
                }
                
                // Reset the cloned flags since binding values are used in multiple bindings
                m_in_binding.resetCloned();
                
                // Evaluate the scan term, which could also be a parameter
                Term term = this.getOperator().getTerm();
                Value scan_value = TermEvaluator.evaluate(term, m_in_binding).getValue();
                
                // Check if the value is missing
                if (scan_value instanceof NullValue)
                {
                    // Keep the tuple list empty
                    next_value = null;
                }
                else if (scan_value instanceof CollectionValue)
                {
                    m_element_list.addAll(((CollectionValue) scan_value).getValues());
                    next_value = m_element_list.poll();
                }
                else
                {
                    throw new AssertionError();
                }
                
                if (next_value == null)
                {
                    // Keep fetching input bindings until the query path evaluation is not empty (inner join semantics)
                    if (this.getOperator().getFlattenSemantics() == Scan.FlattenSemantics.INNER)
                    {
                        continue;
                    }
                    // Create a null value (outer join semantics)
                    else
                    {
                        Type term_type = this.getOperator().getAliasVariable().getType();
                        next_value = new NullValue(term_type.getClass());
                    }
                }
                
                break;
            }
        }
        
        // Create a new binding
        Binding binding = new Binding();
        
        for (BindingValue b_value : m_in_binding.getValues())
        {
            b_value.resetCloned();
            binding.addValue(b_value);
        }
        
        binding.addValue(new BindingValue(next_value, false));
        
        if (this.getOperator().getOrderVariable() != null)
        {
            binding.addValue(new BindingValue(new LongValue(m_order_counter++), true));
        }
        
        return binding;
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        if (this.getState() == State.OPEN)
        {
            m_element_list = null;
            m_in_binding = null;
        }
        
        super.close();
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        ScanImpl copy = new ScanImpl(getOperator(), getChild().copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitScanImpl(this);
    }
    
}
