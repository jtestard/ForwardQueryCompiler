/**
 * 
 */
package edu.ucsd.forward.data.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * An index implemented as a TreeMap (red-black tree), where the key is a single binding value (primitive values only), and the
 * value is a list of bindings. Arithmetic predicates (<, <=, >, >=) are supported, but the key must be a single value. The search
 * time for the TreeMap implementation is higher than the one for HashMap implementation.
 * 
 * @author Yupeng
 * 
 */
public class IndexRedBlack extends AbstractIndex
{
    @SuppressWarnings("unused")
    private static final Logger                      log = Logger.getLogger(IndexRedBlack.class);
    
    /**
     * The index implementation.
     */
    private TreeMap<Binding, Collection<TupleValue>> m_index;
    
    /**
     * Constructs the index with the declaration.
     * 
     * @param declaration
     *            the index declaration.
     */
    public IndexRedBlack(IndexDeclaration declaration)
    {
        super(declaration);
        // Check the index declaration only has one key
        assert declaration.getKeys().size() == 1;
        m_index = new TreeMap<Binding, Collection<TupleValue>>();
    }
    
    @Override
    public List<TupleValue> get(List<KeyRange> ranges)
    {
        assert ranges.size() == 1;
        KeyRange range = ranges.get(0);
        
        assert range.getLowerBound() != null || range.getUpperBound() != null;
        
        List<TupleValue> results = new ArrayList<TupleValue>();
        
        if (range.getLowerBound() != null && range.getUpperBound() != null && range.getLowerBound().equals(range.getUpperBound()))
        {
            // Get only
            if (!range.isLowerOpen() && !range.isUpperOpen())
            {
                Binding to_key = new Binding();
                to_key.addValue(new BindingValue(range.getUpperBound(), false));
                results.addAll(m_index.get(to_key));
            }
            return results;
        }
        else if (range.getLowerBound() == null)
        {
            // Construct the key
            Binding to_key = new Binding();
            to_key.addValue(new BindingValue(range.getUpperBound(), false));
            results.addAll(consolidate(m_index.headMap(to_key).values()));
            if (!range.isUpperOpen())
            {
                Collection<TupleValue> equal_upper_tuples = m_index.get(to_key);
                if (equal_upper_tuples != null) results.addAll(equal_upper_tuples);
            }
        }
        else if (range.getUpperBound() == null)
        {
            // Construct the key
            Binding from_key = new Binding();
            from_key.addValue(new BindingValue(range.getLowerBound(), false));
            results.addAll(consolidate(m_index.tailMap(from_key).values()));
            if (range.isLowerOpen())
            {
                Collection<TupleValue> equal_lower_tuples = m_index.get(from_key);
                if (equal_lower_tuples != null)
                {
                    // FIXME: the equals method of binding is based on set-semantic.
                    results.removeAll(equal_lower_tuples);
                }
            }
        }
        else
        {
            // Construct the key
            Binding to_key = new Binding();
            to_key.addValue(new BindingValue(range.getUpperBound(), false));
            Binding from_key = new Binding();
            from_key.addValue(new BindingValue(range.getLowerBound(), false));
            results.addAll(consolidate(m_index.subMap(from_key, to_key).values()));
            if (!range.isUpperOpen())
            {
                Collection<TupleValue> equal_upper_tuples = m_index.get(to_key);
                if (equal_upper_tuples != null) results.addAll(equal_upper_tuples);
            }
            if (range.isLowerOpen())
            {
                Collection<TupleValue> equal_lower_tuples = m_index.get(from_key);
                if (equal_lower_tuples != null)
                {
                    // FIXME: the equals method of binding is based on set-semantic.
                    results.removeAll(equal_lower_tuples);
                }
            }
        }
        
        return results;
    }
    
    @Override
    public void delete(TupleValue tuple)
    {
        // Construct the key
        Binding key = new Binding();
        List<ScalarValue> keys = computeKeys(tuple);
        assert keys.size() == 1;
        key.addValue(new BindingValue(keys.get(0), false));
        
        // Remove the binding from the bucket
        Collection<TupleValue> bucket = m_index.get(key);
        boolean removed = bucket.remove(tuple);
        assert (removed);
        
        // Remove the bucket if empty
        if (bucket.isEmpty()) m_index.remove(key);
    }
    
    @Override
    public void insert(TupleValue tuple)
    {
        // Construct the key
        Binding key = new Binding();
        List<ScalarValue> keys = computeKeys(tuple);
        assert keys.size() == 1;
        key.addValue(new BindingValue(keys.get(0), false));
        
        Collection<TupleValue> bucket = m_index.get(key);
        if (bucket == null)
        {
            bucket = new ArrayList<TupleValue>();
            m_index.put(key, bucket);
        }
        bucket.add(tuple);
    }
    
    @Override
    public void clear()
    {
        m_index.clear();
    }
    
    @Override
    public boolean isEmpty()
    {
        return m_index.isEmpty();
    }
    
    /**
     * Consolidates the input collection of tuple collections into a single collection of tuples.
     * 
     * @param values
     *            a collection of binding tuples.
     * @return a list of tuples.
     */
    private List<TupleValue> consolidate(Collection<Collection<TupleValue>> values)
    {
        List<TupleValue> result = new ArrayList<TupleValue>();
        for (Collection<TupleValue> value : values)
            result.addAll(value);
        
        return result;
    }
    
    @Override
    public int size()
    {
        return m_index.size();
    }
    
}
