/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.Sort.Item;

/**
 * A class representing a binding and a list of binding values corresponding to the sort items of the sort operator and partition-by
 * operator. This class is passed to the comparator used by the priority queue.
 * 
 * @author Yupeng Fu
 * 
 */
class SortableBinding
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(SortableBinding.class);
    
    private Binding             m_binding;
    
    private List<BindingValue>  m_sort_values;
    
    /**
     * Constructor.
     * 
     * @param binding
     *            the binding to sort.
     * @param sort_items
     *            the sort sort items that specify the order.
     * @throws QueryExecutionException
     *             if there is an error evaluating the sort values.
     */
    public SortableBinding(Binding binding, List<Item> sort_items) throws QueryExecutionException
    {
        m_binding = binding;
        m_sort_values = new ArrayList<BindingValue>();
        
        for (Item sort_item : sort_items)
        {
            BindingValue val = TermEvaluator.evaluate(sort_item.getTerm(), binding);
            m_sort_values.add(val);
        }
    }
    
    /**
     * Gets the binding.
     * 
     * @return the binding.
     */
    public Binding getBinding()
    {
        return m_binding;
    }
    
    /**
     * Gets the sort values.
     * 
     * @return the sort values.
     */
    public List<BindingValue> getSortValues()
    {
        return m_sort_values;
    }
}
