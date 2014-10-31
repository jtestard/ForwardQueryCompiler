/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.Comparator;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.query.ast.OrderByItem.Nulls;
import edu.ucsd.forward.query.ast.OrderByItem.Spec;
import edu.ucsd.forward.query.logical.Sort.Item;

/**
 * A comparator class which imposes a total ordering on the collection of the bindings. This class is passed to the sort
 * implementation to sort bindings.
 * 
 * @author Yupeng Fu
 * 
 */
public class BindingComparator implements Comparator<SortableBinding>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(BindingComparator.class);
    
    private List<Item>          m_sort_items;
    
    /**
     * Constructs the comparator with the sort items which specifies the ordering.
     * 
     * @param sort_tiems
     *            the sort operator that specifies the ordering
     */
    public BindingComparator(List<Item> sort_items)
    {
        m_sort_items = sort_items;
    }
    
    @Override
    public int compare(SortableBinding o1, SortableBinding o2)
    {
        assert o1 != null && o2 != null;
        
        BindingValue v1 = null;
        BindingValue v2 = null;
        Item sort_item;
        
        int result = 0;
        for (int i = 0; i < m_sort_items.size(); i++)
        {
            sort_item = m_sort_items.get(i);
            v1 = o1.getSortValues().get(i);
            v2 = o2.getSortValues().get(i);
            
            // Handle null values
            if (v1.getValue() instanceof NullValue && v2.getValue() instanceof NullValue) continue;
            else if (v1.getValue() instanceof NullValue && !(v2.getValue() instanceof NullValue)) result = -1;
            else if (!(v1.getValue() instanceof NullValue) && v2.getValue() instanceof NullValue) result = 1;
            
            if (result != 0)
            {
                // Determine whether nulls appear after non-null values.
                if (sort_item.getNulls() == Nulls.LAST) result *= -1;
                return result;
            }
            
            // Compare binding value
            result = v1.compareTo(v2);
            if (result != 0)
            {
                // Set the sort direction
                if (sort_item.getSpec() == Spec.DESC) result *= -1;
                return result;
            }
        }
        return result;
    }
}
