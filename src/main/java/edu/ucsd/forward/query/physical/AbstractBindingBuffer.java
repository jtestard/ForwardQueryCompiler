/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * An abstract implementation of the binding buffer interface.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 * @author Vicky Papavasileiou
 */
public abstract class AbstractBindingBuffer implements BindingBuffer
{
    /**
     * Holds the output info of the bindings being indexed.
     */
    private OutputInfo          m_left_info;
    
    /**
     * Holds the output info of the bindings probing the index.
     */
    private OutputInfo          m_right_info;
    
    /**
     * Indicates if it is in the middle of population.
     */
    private boolean             m_in_population;
    
    /**
     * Holds the conditions used to match bindings.
     */
    private Collection<Term>    m_conjuncts;
    
    /**
     * Determines whether to keep track of matched bindings.
     */
    private boolean             m_track_matched_bindings;
    
    /**
     * Holds the matched bindings.
     */
    private Collection<Binding> m_matched_bindings;
    
    /**
     * Constructor.
     * 
     * @param conjuncts
     *            the conditions used to match bindings.
     * @param left_info
     *            the output info of the bindings being indexed.
     * @param right_info
     *            the output info of the bindings probing the index.
     * @param track_matched_bindings
     *            determines whether to keep track of matched bindings.
     */
    protected AbstractBindingBuffer(Collection<Term> conjuncts, OutputInfo left_info, OutputInfo right_info,
            boolean track_matched_bindings)
    {
        assert (conjuncts != null);
        assert (left_info != null);
        assert (right_info != null);
        
        m_conjuncts = conjuncts;
        m_left_info = left_info;
        m_right_info = right_info;
        m_in_population = false;
        
        m_track_matched_bindings = track_matched_bindings;
        if (m_track_matched_bindings) m_matched_bindings = new ArrayList<Binding>();
    }
    
    @Override
    public void populate(OperatorImpl op_impl) throws QueryExecutionException, SuspensionException
    {
        if (!m_in_population)
        {
            clear();
            m_in_population = true;
        }
        
        Binding binding = op_impl.next();
        while (binding != null)
        {
            add(binding);
            binding = op_impl.next();
        }
        
        m_in_population = false;
    }
    
    @Override
    public void clear()
    {
        if (m_track_matched_bindings) m_matched_bindings.clear();
    }
    
    @Override
    public void clearMatchedBindings()
    {
        assert (m_track_matched_bindings);
        
        m_matched_bindings.clear();
    }
    
    @Override
    public Iterator<Binding> getMatchedBindings()
    {
        assert (m_track_matched_bindings);
        
        return m_matched_bindings.iterator();
    }
    
    @Override
    public List<Binding> getAllMatchedBindings()
    {
        assert (m_track_matched_bindings);
        
        return new ArrayList<Binding>(m_matched_bindings);
    }    
    
    /**
     * Gets the output info of the bindings being indexed.
     * 
     * @return output info of the bindings being indexed.
     */
    protected OutputInfo getLeftInfo()
    {
        return m_left_info;
    }
    
    /**
     * Gets the output info of the bindings probing the index.
     * 
     * @return the output info of the bindings probing the index.
     */
    public OutputInfo getRightInfo()
    {
        return m_right_info;
    }
    
    /**
     * Applies the matching conditions to the left (buffer) bindings and the right (probing) binding, and outputs either the left
     * bindings or the concatenation of left bindings and the right binding that satisfy them.
     * 
     * @param left_bindings
     *            bindings for the buffer.
     * @param right_binding
     *            the probing binding.
     * @param concatenate
     *            whether to output only left bindings or the concatenation of left bindings and the right binding.
     * @return a collection of bindings that satisfy the matching conditions.
     * @throws QueryExecutionException
     *             if an exception is raised during term evaluation.
     */
    protected Collection<Binding> applyConditions(Collection<Binding> left_bindings, Binding right_binding, boolean concatenate)
            throws QueryExecutionException
    {
        assert (left_bindings != null);
        assert (right_binding != null);
        
        // No conditions to apply
        if (this.m_conjuncts.isEmpty() && !concatenate)
        {
            if (m_track_matched_bindings) this.m_matched_bindings.addAll(left_bindings);
            
            return left_bindings;
        }
        
        Collection<Binding> true_bindings = new ArrayList<Binding>();
        boolean failed = false;
        
        for (Binding left_binding : left_bindings)
        {
            failed = false;
            
            // Construct a big binding from the current bindings
            Binding out_binding = new Binding();
            out_binding.addValues(left_binding.getValues());
            out_binding.addValues(right_binding.getValues());
            
            // Evaluate all conditions
            for (Term term : m_conjuncts)
            {
                Value result = TermEvaluator.evaluate(term, out_binding).getValue();
                
                if (result instanceof NullValue || !((BooleanValue) result).getObject())
                {
                    failed = true;
                    break;
                }
            }
            
            if (!failed)
            {
                if (m_track_matched_bindings) this.m_matched_bindings.add(left_binding);
                
                // Add the left binding to the output list
                if (!concatenate) true_bindings.add(left_binding);
                // Add the concatenated binding to the output list
                else true_bindings.add(out_binding);
            }
        }
        
        return true_bindings;
    }
    
}
