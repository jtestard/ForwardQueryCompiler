/**
 * 
 */
package edu.ucsd.forward.data.value;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.constraint.OrderByAttribute;
import edu.ucsd.forward.data.constraint.OrderConstraint;
import edu.ucsd.forward.data.java.JavaToSqlMapUtil;
import edu.ucsd.forward.data.json.JsonToValueConverter;
import edu.ucsd.forward.data.type.TypeException;
import edu.ucsd.forward.query.ast.OrderByItem.Nulls;
import edu.ucsd.forward.query.ast.OrderByItem.Spec;

/**
 * A comparator class which imposes a total ordering on the collection of the tuple values.
 * 
 * @author Yupeng Fu
 * 
 */
public class TupleValueComparator implements Comparator<TupleValue>
{
    @SuppressWarnings("unused")
    private static final Logger           log = Logger.getLogger(TupleValueComparator.class);
    
    private OrderConstraint               m_constraint;
    
    private Map<String, OrderByAttribute> m_path_attr_map;
    
    /**
     * Constructs the comparator with the order by constraint.
     * 
     * @param constraint
     *            the order constraint that specifies the ordering
     */
    public TupleValueComparator(OrderConstraint constraint)
    {
        assert constraint != null;
        m_constraint = constraint;
        m_path_attr_map = new LinkedHashMap<String, OrderByAttribute>();
        for (OrderByAttribute attr : m_constraint.getOrderByAttributes())
        {
            SchemaPath path = new SchemaPath(m_constraint.getCollectionType(), attr.getAttribute());
            m_path_attr_map.put(path.getLastPathStep(), attr);
        }
    }
    
    @Override
    public int compare(TupleValue t1, TupleValue t2)
    {
        assert t1 != null && t2 != null;
        
        Value v1 = null;
        Value v2 = null;
        OrderByAttribute attr;
        
        int result = 0;
        for (String path : m_path_attr_map.keySet())
        {
            attr = m_path_attr_map.get(path);
            v1 = t1.getAttribute(path);
            v2 = t2.getAttribute(path);
            
            // Handle null values
            if (v1 instanceof NullValue && v2 instanceof NullValue) continue;
            else if (v1 instanceof NullValue && !(v2 instanceof NullValue)) result = -1;
            else if (!(v1 instanceof NullValue) && v2 instanceof NullValue) result = 1;
            
            if (result != 0)
            {
                // Determine whether nulls appear after non-null values.
                if (attr.getNulls() == Nulls.LAST) result *= -1;
                return result;
            }
            
            // Compare value
            result = compare(v1, v2);
            if (result != 0)
            {
                // Set the sort direction
                if (attr.getSpec() == Spec.DESC) result *= -1;
                return result;
            }
        }
        return result;
    }
    
    /**
     * Compare two values.
     * 
     * @param in_left
     *            the left
     * @param in_right
     *            the right
     * @return the comparison result.
     */
    private int compare(Value in_left, Value in_right)
    {
        Value left = in_left;
        Value right = in_right;
        if (left instanceof JsonValue)
        {
            left = getSqlValue(in_left);
        }
        
        if (right instanceof JsonValue)
        {
            right = getSqlValue(in_right);
        }
        
        // NULL values
        if (left instanceof NullValue && right instanceof NullValue) return 0;
        // Left NULL value only
        else if (left instanceof NullValue) return -1;
        // Right NULL value only
        else if (right instanceof NullValue) return 1;
        
        // Unable to compare complex values
        if (!(left instanceof PrimitiveValue<?>) || !(right instanceof PrimitiveValue<?>))
        {
            // Complex and scalar values are not supported
            throw new UnsupportedOperationException();
        }
        
        assert (left.getTypeClass() == right.getTypeClass());
        
        return ((PrimitiveValue) left).getObject().compareTo(((PrimitiveValue) right).getObject());
    }
    
    /**
     * Returns the value in SQL value. This will cast Java value into SQL in a best-effort way.
     * 
     * @param v
     *            the value to cast
     * 
     * @return the value converted to SQL value.
     */
    public Value getSqlValue(Value v)
    {
        if (v instanceof JsonValue)
        {
            JsonValue json_value = (JsonValue) v;
            try
            {
                return JsonToValueConverter.mapScalar(json_value);
            }
            catch (TypeException e)
            {
                throw new UnsupportedOperationException();
            }
        }
        return v;
    }
}
