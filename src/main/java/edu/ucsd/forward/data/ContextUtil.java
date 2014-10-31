/**
 * 
 */
package edu.ucsd.forward.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.constraint.PrimaryKeyUtil;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;

/**
 * Utility methods for getting the context of values.
 * 
 * @author Kian Win
 * 
 */
public final class ContextUtil
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ContextUtil.class);
    
    /**
     * Private constructor.
     */
    private ContextUtil()
    {
        
    }
    
    /**
     * Returns the context of a type instance as a list of scalar types. The list of scalar types is the global primary key of the
     * closest ancestor relation type.
     * 
     * @param type
     *            - the type instance.
     * @return the context as a list of scalar types.
     */
    public static List<ScalarType> getContextAsType(Type type)
    {
        // Find the closest ancestor relation type
        CollectionType relation_type = type.getClosestAncestor(CollectionType.class);
        
        // If there is no ancestor relation type, there are no global primary key attributes to return
        if (relation_type == null) return Collections.emptyList();
        
        return PrimaryKeyUtil.getGlobalPrimaryKeyTypes(relation_type);
        
    }
    
    /**
     * Returns the context of a value.
     * 
     * @param value
     *            the value.
     * @return the context value.
     */
    public static ContextValue getContextAsValue(Value value)
    {
        assert value != null;
        
        DataTree data_tree = value.getDataTree();
        assert data_tree != null;
        
        List<ScalarValue> scalar_values = new ArrayList<ScalarValue>();
        
        // Find the closest ancestor relation type
        CollectionType relation_type = value.getType().getClosestAncestor(CollectionType.class);
        
        // If there is no ancestor relation type, there are no global primary key attributes to return
        if (relation_type == null) return new ContextValue(data_tree, scalar_values);
        
        // For each global primary key attribute of the relation type
        List<ScalarType> global_key = PrimaryKeyUtil.getGlobalPrimaryKeyTypes(relation_type);
        
        for (ScalarType key_attribute : global_key)
        {
            // Find the corresponding value that is closest to what we are calculating the context for
            scalar_values.add(findClosestKeyAttribute(value, key_attribute));
        }
        return new ContextValue(data_tree, scalar_values);
    }
    
    /**
     * Given a value, find the closest scalar value that corresponds to a given global primary key attribute.
     * 
     * @param value
     *            the value.
     * @param type
     *            - the global primary key attribute type.
     * @return the closest scalar value.
     */
    protected static ScalarValue findClosestKeyAttribute(Value value, ScalarType type)
    {
        assert value != null;
        assert type != null;
        
        /*
         * The closest value has a root-to-node path that overlaps maximally with the root-to-node path of the value.
         */

        // The roots of the data tree and type tree must correspond to each other
        assert value.getDataTree().getType() == type.getSchemaTree();
        
        /*
         * First, find the highest value ancestor that does not correspond to the type ancestor.
         */

        List<Value> value_path = value.getAncestors();
        value_path.add(value);
        
        List<Type> type_path = type.getAncestors();
        type_path.add(type);
        
        int diff_pos = -1;
        for (int i = 0; i < type_path.size(); i++)
        {
            if (i >= value_path.size() || value_path.get(i).getType() != type_path.get(i))
            {
                // Found the earliest difference, either because there are no more value ancestors, or the value ancestor does not
                // correspond to the type ancestor
                diff_pos = i;
                break;
            }
        }
        
        // If there is no difference, the value itself correspond to the specified global primary key attribute
        if (diff_pos == -1) return (ScalarValue) value;
        
        /*
         * Otherwise, we start navigating the data tree from the point right before we discover the difference. There is definitely
         * one such point, because the roots of the data tree and type tree correspond to each other.
         */

        assert diff_pos > 0;
        Value next_value = value_path.get(diff_pos - 1);
        
        // At each step, the value node and type node at position i correspond to each other, and we want to advance to position
        // i+1.
        for (int i = diff_pos - 1; i < type_path.size() - 1; i++)
        {
            Value cur_value = next_value;
            Type cur_type = type_path.get(i);
            Type next_type = type_path.get(i + 1);
            
            assert cur_value.getType() == cur_type;
            
            // Since global primary key attributes can only be direct or indirect attributes, we will only navigate through nested
            // tuples.
            assert cur_type instanceof TupleType;
            String attribute_name = ((TupleType) cur_type).getAttributeName((Type) next_type);
            next_value = ((TupleValue) cur_value).getAttribute(attribute_name);
            assert next_value.getType() == next_type;
        }
        
        // The global primary key attribute is a scalar type, therefore the value found must be scalar.
        
        return (ScalarValue) next_value;
    }
}
