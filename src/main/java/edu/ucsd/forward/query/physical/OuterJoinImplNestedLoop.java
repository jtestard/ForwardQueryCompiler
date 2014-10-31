package edu.ucsd.forward.query.physical;

import java.util.Collection;
import java.util.Iterator;

import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.OuterJoin;
import edu.ucsd.forward.query.logical.OuterJoin.Variation;
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * The implementation of the nested-loop algorithm for the outer join logical operator. This operator implementation buffers the
 * values of the right child operator implementation on demand.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 */
public class OuterJoinImplNestedLoop extends AbstractBinaryOperatorImplNestedLoop<OuterJoin>
{
    /**
     * The flag that indicates if the current right binding has been matched.
     */
    private boolean m_right_matched;
    
    /**
     * Determines whether the right side is exhausted.
     */
    private boolean m_right_exhausted;
    
    /**
     * Determines whether the population of the left side is done.
     */
    private boolean m_population_done;
    
    /**
     * Constructs an instance of the nested-loop join operator implementation.
     * 
     * @param logical
     *            a logical join operator.
     * @param left_child
     *            the left child operator implementation.
     * @param right_child
     *            the right child operator implementation.
     */
    public OuterJoinImplNestedLoop(OuterJoin logical, OperatorImpl left_child, OperatorImpl right_child)
    {
        super(logical, left_child, right_child);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        // Construct the binding buffer.
        m_right_matched = false;
        m_right_exhausted = false;
        m_population_done = false;
        
        BindingBufferFactory factory = BindingBufferFactory.getInstance();
        
        OutputInfo left_info = this.getLeftChild().getOperator().getOutputInfo();
        OutputInfo right_info = this.getRightChild().getOperator().getOutputInfo();
        Collection<Term> join_conditions = this.getOperator().getConditions();
        
        BindingBuffer buffer = factory.buildBindingBuffer(join_conditions, left_info, right_info, true);
        this.setLeftBindingBuffer(buffer);
        
        super.open();
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        // Populate the left buffer
        if (!m_population_done)
        {
            this.getLeftBindingBuffer().populate(this.getLeftChild());
            m_population_done = true;
        }
        
        // Get the outer join variation
        Variation variation = this.getOperator().getOuterJoinVariation();
        
        // Fail fast if there is no left binding and it is a LEFT OUTER
        if (this.getLeftBindingBuffer().isEmpty() && variation == Variation.LEFT)
        {
            return null;
        }
        
        while (!m_right_exhausted)
        {
            // Get the next right value
            if (this.getRightBinding() == null || !this.getLeftIterator().hasNext())
            {
                // Output the unmatched right binding in case of FULL OUTER and RIGHT OUTER
                if (!m_right_matched && getRightBinding() != null && (variation == Variation.FULL || variation == Variation.RIGHT))
                {
                    // Set the match flag
                    m_right_matched = true;
                    
                    Binding out_binding = new Binding();
                    // Pad left part with null values
                    OutputInfo left_output_info = this.getLeftChild().getOperator().getOutputInfo();
                    for (RelativeVariable left_var : left_output_info.getVariables())
                    {
                        // assert (left_var.getType() instanceof TupleType); Needed for old scalar
                        
                        BindingValue null_value = new BindingValue(new NullValue(left_var.getType().getClass()), false);
                        out_binding.addValue(null_value);
                    }
                    // Copy the right values
                    out_binding.addValues(this.getRightBinding().getValues());
                    
                    // No need to reset the cloned flags since the binding does not have a match
                    return out_binding;
                }
                
                // Retrieve the next right binding and reset the match flag
                this.setRightBinding(this.getRightChild().next());
                m_right_matched = false;
                
                // The right side is exhausted
                if (getRightBinding() == null)
                {
                    m_right_exhausted = true;
                    
                    // Set the left iterator to the remaining left bindings in order to output more tuples in case of LEFT OUTER and
                    // FULL OUTER
                    this.setLeftIterator(this.getLeftBindingBuffer().getBindings());
                    
                    break;
                }
                
                // Reset and the left iterator
                this.setLeftIterator(this.getLeftBindingBuffer().get(getRightBinding(), true));
            }
            
            // There are no matching bindings
            if (!this.getLeftIterator().hasNext()) continue;
            
            // Get the next matching binding
            Binding out_binding = this.getLeftIterator().next();
            
            // Set the match flag
            m_right_matched = true;
            
            // Reset the cloned flags since binding values are used in multiple bindings
            out_binding.resetCloned();
            
            return out_binding;
        }
        
        // Output the remaining bindings in the left buffer in case of LEFT OUTER and FULL OUTER
        if (variation == Variation.LEFT || variation == Variation.FULL)
        {
            while (this.getLeftIterator().hasNext())
            {
                Binding left_binding = this.getLeftIterator().next();
                
                // Check if the left binding was matched with a right binding
                Iterator<Binding> matched_bindings = this.getLeftBindingBuffer().getMatchedBindings();
                boolean matched = false;
                while (matched_bindings.hasNext())
                    if (matched_bindings.next() == left_binding)
                    {
                        matched = true;
                        break;
                    }
                if (matched) continue;
                
                Binding out_binding = new Binding();
                // Copy the left values
                out_binding.addValues(left_binding.getValues());
                // Pad right part with null values
                OutputInfo right_output_info = this.getRightChild().getOperator().getOutputInfo();
                for (RelativeVariable right_var : right_output_info.getVariables())
                {
                    BindingValue null_value = new BindingValue(new NullValue(right_var.getType().getClass()), false);
                    out_binding.addValue(null_value);
                }
                
                // No need to reset the cloned flags since the binding does not have a match
                return out_binding;
            }
        }
        
        return null;
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        OuterJoinImplNestedLoop copy = new OuterJoinImplNestedLoop(this.getOperator(), this.getLeftChild().copy(context),
                                                                   this.getRightChild().copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitOuterJoinImplNestedLoop(this);
    }
    
}
