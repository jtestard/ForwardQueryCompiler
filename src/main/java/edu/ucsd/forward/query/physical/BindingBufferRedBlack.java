/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.Function;
import edu.ucsd.forward.query.function.comparison.GreaterEqualFunction;
import edu.ucsd.forward.query.function.comparison.GreaterThanFunction;
import edu.ucsd.forward.query.function.comparison.LessEqualFunction;
import edu.ucsd.forward.query.function.comparison.LessThanFunction;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.term.Term;

/**
 * The buffer is implemented as a TreeMap (red-black tree), where the key is a single binding value (primitive values only), and the
 * value is a list of bindings. Arithmetic predicates (<, <=, >, >=) are supported, but the key must be a single binding value. The
 * search time for the TreeMap implementation is higher than the one for HashMap implementation.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class BindingBufferRedBlack extends AbstractBindingBuffer
{
    @SuppressWarnings("unused")
    private static final Logger                   log = Logger.getLogger(BindingBufferRedBlack.class);
    
    /**
     * Holds the predicate between two terms, the left one containing query paths from the bindings being indexed and the right one
     * from the bindings that probe the index.
     */
    private GeneralFunctionCall                   m_index_predicate;
    
    /**
     * The index implementation.
     */
    private TreeMap<Binding, Collection<Binding>> m_index;
    
    /**
     * Constructor.
     * 
     * @param index_predicate
     *            a predicate between two terms, the left one containing query paths from the bindings being indexed and the right
     *            one from the bindings that probe the index.
     * @param conditions
     *            the conditions used to match bindings.
     * @param left_info
     *            the output info of the bindings being indexed.
     * @param right_info
     *            the output info of the bindings probing the index.
     * @param track_matched_bindings
     *            determines whether to keep track of matched bindings.
     */
    protected BindingBufferRedBlack(GeneralFunctionCall index_predicate, Collection<Term> conditions, OutputInfo left_info,
            OutputInfo right_info, boolean track_matched_bindings)
    {
        super(conditions, left_info, right_info, track_matched_bindings);
        
        assert (index_predicate != null);
        
        m_index_predicate = index_predicate;
        
        m_index = new TreeMap<Binding, Collection<Binding>>();
    }
    
    @Override
    public void clear()
    {
        super.clear();
        
        m_index.clear();
    }
    
    @Override
    public Iterator<Binding> get(Binding binding, boolean concatenate) throws QueryExecutionException
    {
        // Construct the key
        Term right_term = m_index_predicate.getArguments().get(1);
        Binding key = new Binding();
        // Pad the right binding with as many nulls as the size of left bindings in order for the binding indexes of the
        // query paths to be valid during evaluation.
        Binding right_binding = new Binding();
        for (int i = 0; i < this.getLeftInfo().size(); i++)
        {
            right_binding.addValue(null);
        }
        right_binding.addValues(binding.getValues());
        key.addValue(TermEvaluator.evaluate(right_term, right_binding));
        
        // Access the index to retrieve the left bindings
        List<Binding> left_bindings;
        Function function = m_index_predicate.getFunction();
        if (function instanceof LessThanFunction)
        {
            left_bindings = consolidate(m_index.headMap(key).values());
        }
        else if (function instanceof LessEqualFunction)
        {
            left_bindings = consolidate(m_index.headMap(key).values());
            Collection<Binding> equal_left_bindings = m_index.get(key);
            if (equal_left_bindings != null)
            {
                left_bindings.addAll(equal_left_bindings);
            }
        }
        else if (function instanceof GreaterThanFunction)
        {
            left_bindings = consolidate(m_index.tailMap(key).values());
            Collection<Binding> equal_left_bindings = m_index.get(key);
            if (equal_left_bindings != null)
            {
                left_bindings.removeAll(equal_left_bindings);
            }
        }
        else if (function instanceof GreaterEqualFunction)
        {
            left_bindings = consolidate(m_index.tailMap(key).values());
        }
        else
        {
            throw new AssertionError();
        }
        
        // Apply the rest of the conditions
        return this.applyConditions(left_bindings, binding, concatenate).iterator();
    }
    
    @Override
    public Iterator<Binding> getBindings()
    {
        return consolidate(m_index.values()).iterator();
    }
    
    @Override
    public boolean isEmpty()
    {
        return m_index.isEmpty();
    }
    
    /**
     * Consolidates the input collection of binding collections into a single collection of bindings.
     * 
     * @param values
     *            a collection of binding collections.
     * @return a list of bindings.
     */
    private List<Binding> consolidate(Collection<Collection<Binding>> values)
    {
        List<Binding> result = new ArrayList<Binding>();
        for (Collection<Binding> value : values)
            result.addAll(value);
        
        return result;
    }
    
    @Override
    public void add(Binding binding) throws QueryExecutionException
    {
        // Construct the key
        Term left_term = m_index_predicate.getArguments().get(0);
        Binding key = new Binding();
        key.addValue(TermEvaluator.evaluate(left_term, binding));
        
        Collection<Binding> bucket = m_index.get(key);
        if (bucket == null)
        {
            bucket = new ArrayList<Binding>();
            m_index.put(key, bucket);
        }
        bucket.add(binding);
    }
    
    @Override
    public void remove(Binding binding) throws QueryExecutionException
    {
        // Construct the key
        Term left_term = m_index_predicate.getArguments().get(0);
        Binding key = new Binding();
        key.addValue(TermEvaluator.evaluate(left_term, binding));
        
        // Remove the binding from the bucket
        Collection<Binding> bucket = m_index.get(key);
        // Use iterator instead of bucket.remove(binding) in order to force object equality (==).
        Iterator<Binding> iter = bucket.iterator();
        boolean found = false;
        while (iter.hasNext())
        {
            if (iter.next() == binding)
            {
                iter.remove();
                found = true;
                break;
            }
        }
        assert (found);
        
        // Remove the bucket if empty
        if (bucket.isEmpty()) m_index.remove(key);
    }
    
    @Override
    public int size()
    {
        return m_index.size();
    }

    /**
     * <b>Inherited JavaDoc:</b><br>
     * {@inheritDoc}
     * <br><br>
     * <b>See original method below.</b>
     * <br>
     * @see edu.ucsd.forward.query.physical.BindingBuffer#getAllBindings()
     */
    @Override
    public List<Binding> getAllBindings()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * <b>Inherited JavaDoc:</b><br>
     * {@inheritDoc}
     * <br><br>
     * <b>See original method below.</b>
     * <br>
     * @see edu.ucsd.forward.query.physical.BindingBuffer#getUnmatchedBindings()
     */
    @Override
    public Iterator<Binding> getUnmatchedBindings()
    {
        // TODO Auto-generated method stub
        return null;
    }
    
}
