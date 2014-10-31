/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import edu.ucsd.app2you.util.collection.Pair;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.term.Term;

/**
 * The buffer is implemented as a HashMap, where the key is a list of binding values, and the value is a list of bindings. The
 * search time for The HashMap implementation is lower than the one for TreeMap (red-black tree) implementation, on average, and the
 * key can be any number of binding values, but only equality predicates are supported.
 * 
 * @author Michalis Petropoulos
 * @author Vicky Papavasileiou
 */
public class BindingBufferHash extends AbstractBindingBuffer
{
    @SuppressWarnings("unused")
    private static final Logger                                  log = Logger.getLogger(BindingBufferHash.class);
    
    /**
     * Holds the terms to be evaluated when adding a binding to the buffer.
     */
    private List<Term>                                           m_left_terms;
    
    /**
     * Holds the terms to be evaluated when probing the buffer with a binding.
     */
    private List<Term>                                           m_right_terms;
    
    /**
     * The index implementation.
     */
    private HashMap<Binding, Collection<Binding>>                m_index;
    
    private HashMap<Binding, Pair<Collection<Binding>, Boolean>> m_index_test;
    
    /**
     * Determines whether null matches should be dropped. This is needed because the buffer is used by binary operators such as
     * join, that drops null matches, and unary operators such as union, that does not drop null matches.
     */
    private boolean                                              m_binary;
    
    /**
     * Constructor.
     * 
     * @param left_terms
     *            the terms to be evaluated when adding a binding to the buffer.
     * @param right_terms
     *            the terms to be evaluated when probing the buffer with a binding.
     * @param conditions
     *            other conditions (non-indexable) to check before returning any bindings.
     * @param left_info
     *            the output info of the bindings being indexed.
     * @param right_info
     *            the output info of the bindings probing the index.
     * @param track_matched_bindings
     *            determines whether to keep track of matched bindings.
     */
    protected BindingBufferHash(List<Term> left_terms, List<Term> right_terms, Collection<Term> conditions, OutputInfo left_info,
            OutputInfo right_info, boolean track_matched_bindings)
    {
        super(conditions, left_info, right_info, track_matched_bindings);
        
        assert (left_terms != null);
        assert (right_terms != null);
        
        m_binary = true;
        
        m_left_terms = left_terms;
        m_right_terms = right_terms;
        
        m_index = new LinkedHashMap<Binding, Collection<Binding>>();
        m_index_test = new LinkedHashMap<Binding, Pair<Collection<Binding>,Boolean>>();
    }
    
    /**
     * Constructor.
     * 
     * @param terms
     *            the terms to be evaluated when adding and probing the buffer.
     * @param info
     *            the output info of the bindings being indexed and probed.
     * @param track_matched_bindings
     *            determines whether to keep track of matched bindings.
     */
    public BindingBufferHash(List<Term> terms, OutputInfo info, boolean track_matched_bindings)
    {
        super(Collections.<Term> emptyList(), info, info, track_matched_bindings);
        
        assert (terms != null);
        
        m_binary = false;
        
        m_left_terms = terms;
        m_right_terms = terms;
        
        m_index = new LinkedHashMap<Binding, Collection<Binding>>();
        m_index_test = new LinkedHashMap<Binding, Pair<Collection<Binding>,Boolean>>();
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
        Binding key = new Binding();
        BindingValue val;
        for (Term right_term : m_right_terms)
        {
            Binding right_binding = binding;
            if (m_binary)
            {
                // Pad the right binding with as many nulls as the size of left bindings in order for the binding indexes of the
                // query paths to be valid during evaluation.
                right_binding = new Binding();
                for (int i = 0; i < this.getLeftInfo().size(); i++)
                {
                    right_binding.addValue(null);
                }
                right_binding.addValues(binding.getValues());
            }
            
            val = TermEvaluator.evaluate(right_term, right_binding);
            
            if (val.getValue() instanceof NullValue && m_binary)
            {
                return new ArrayList<Binding>().iterator();
            }
            
            key.addValue(val);
        }
        
        Collection<Binding> left_bindings = m_index.get(key);
        
        // No bindings found
        if (left_bindings == null) return Collections.<Binding> emptyList().iterator();
        m_index_test.get(key).setValue(true);
        // Apply the rest of the conditions
        return this.applyConditions(left_bindings, binding, concatenate).iterator();        
    }
    
    @Override
    public Iterator<Binding> getBindings()
    {
        return consolidate(m_index.values()).iterator();
    }
    
    @Override
    public List<Binding> getAllBindings()
    {
        return new ArrayList<Binding>(consolidate(m_index.values()));
    }
    
    @Override
    public Iterator<Binding> getUnmatchedBindings()
    {
        return new Iterator<Binding>() {

            private Pair<Collection<Binding>,Boolean> peekedElement = null;
            private Iterator <Pair<Collection<Binding>, Boolean>> outer_it = m_index_test.values().iterator();
            private Iterator<Binding> inner_it = null;
            
            @Override
            public boolean hasNext()
            {
                return outer_it.hasNext();
            }

            @Override
            public Binding next()
            {
                if(peekedElement == null)
                {
                    peekedElement = outer_it.next();
                }
                
//                if(!peekedElement.getSecond())
//                {
//                    return peekedElement.getFirst().iterator().next();
//                }
//                else
//                {
                while(true)
                {
                    if(inner_it == null)
                    {
                        while(peekedElement.getSecond() == true && outer_it.hasNext())
                        {
                            peekedElement = outer_it.next();
                        }
                        inner_it = peekedElement.getFirst().iterator();
                    }
                    if(!outer_it.hasNext() && peekedElement.getSecond()) return null;
                    
                    if(inner_it.hasNext())
                    {
                        return inner_it.next();
                    }
                    inner_it = null;
                    peekedElement = outer_it.next();
                    continue;
//                }
                }
            }

            @Override
            public void remove()
            {
                throw new RuntimeException("Operation not supported");
            }
        };
        
    }
    
    /**
     * Returns the buckets of bindings.
     * 
     * @return an iterator of buckets of bindings.
     */
    public Iterator<Entry<Binding, Collection<Binding>>> getBindingBuckets()
    {
        return this.m_index.entrySet().iterator();
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
     * @return a collection of bindings.
     */
    private Collection<Binding> consolidate(Collection<Collection<Binding>> values)
    {
        Collection<Binding> result = new ArrayList<Binding>();
        for (Collection<Binding> value : values)
            result.addAll(value);
        
        return result;
    }
    
    @Override
    public void add(Binding binding) throws QueryExecutionException
    {
        // Construct the key
        Binding key = new Binding();
        for (Term left_term : m_left_terms)
            key.addValue(TermEvaluator.evaluate(left_term, binding));
        
        Collection<Binding> bucket = m_index.get(key);
        if (bucket == null)
        {
            bucket = new ArrayList<Binding>();
            m_index.put(key, bucket);
            m_index_test.put(key, new Pair(bucket,false));
        }
        bucket.add(binding);
    }
    
    @Override
    public void remove(Binding binding) throws QueryExecutionException
    {
        // Construct the key
        Binding key = new Binding();
        for (Term left_term : m_left_terms)
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
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.query.physical.BindingBuffer#size()
     */
    @Override
    public int size()
    {
        return m_index.size();
    }
    
}
