/**
 * 
 */
package edu.ucsd.forward.data.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonPrimitive;

import edu.ucsd.app2you.util.identity.IdentityHashSet;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.DataPath;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.NumericValue;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.SwitchValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.TupleValue.AttributeValueEntry;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.data.value.XhtmlValue;

/**
 * Utility class to serialize a value to a JSON object.
 * 
 * The format used is as follows:
 * 
 * <data_tree> ::- [ <value> ]
 * 
 * <value> ::- <tuple_value> | <collection_value> | <switch_value> | <scalar_value>
 * 
 * <tuple_value> ::- [ <value>, ... ]
 * 
 * <collection_value> ::- [ <collection_tuple_value>, ... ]
 * 
 * <collection_tuple_value> ::- [ <predicate>, <value>, ... ]
 * 
 * <switch_value> ::- ["case_name", <tuple_value> ]
 * 
 * <scalar_value> ::- "scalar_value"
 * 
 * <predicate> ::- "[attr=value, ...]"
 * 
 * @author Hongping Lim
 * @author Michalis Petropoulos
 * 
 */
public final class ValueJsonSerializer
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ValueJsonSerializer.class);
    
    /**
     * Hidden constructor.
     */
    private ValueJsonSerializer()
    {
    }
    
    /**
     * Serializes a data tree to a JSON array.
     * 
     * @param data_tree
     *            a data tree to serialize.
     * @return a JSON array.
     */
    public static JsonArray serializeDataTree(DataTree data_tree)
    {
        return serializeDataTree(data_tree, true);
    }
    
    /**
     * Serializes a data tree to a JSON array.
     * 
     * @param data_tree
     *            a data tree to serialize.
     * @param add_predicate_attribute
     *            See {@link #serializeValue(Value, JsonArray, boolean)}.
     * @return a JSON array.
     */
    public static JsonArray serializeDataTree(DataTree data_tree, boolean add_predicate_attribute)
    {
        JsonArray array = new JsonArray();
        serializeValue(data_tree.getRootValue(), array, null, add_predicate_attribute);
        
        return array;
    }
    
    /**
     * Serializes a value to a JSON array. The given set of leaf values indicate where to stop serializing: the leaf (tuple) values
     * will be serialized, but their descendants will not.
     * 
     * @param value
     *            the value to serialize.
     * @param array
     *            the containing JSON array.
     * @param leaf_values
     *            the leaf values.
     */
    public static void serializeValue(Value value, JsonArray array, IdentityHashSet<TupleValue> leaf_values)
    {
        serializeValue(value, array, leaf_values, true);
    }
    
    /**
     * Serializes a value to a JSON array. The given set of leaf values indicate where to stop serializing: the leaf (tuple) values
     * will be serialized, but their descendants will not.
     * 
     * @param value
     *            the value to serialize.
     * @param array
     *            the containing JSON array.
     * @param leaf_values
     *            the leaf values.
     * @param add_predicate_attribute
     *            for tuple values within a collection value, specify whether the resulting element should have an attribute that
     *            stores the predicate that is calculated based on the keys.
     */
    public static void serializeValue(Value value, JsonArray array, IdentityHashSet<TupleValue> leaf_values,
            boolean add_predicate_attribute)
    {
        if (value instanceof TupleValue)
        {
            TupleValue tuple_value = (TupleValue) value;
            
            // Add the tuple value array
            JsonArray tuple_array = new JsonArray();
            array.add(tuple_array);
            
            if (add_predicate_attribute && value.getParent() instanceof CollectionValue)
            {
                // Sets the value of the predicate as an attribute on the element.
                // This is an optimization so that the client runtime doesn't need to recompute
                // the predicate from the keys if the predicate is already provided by the
                // server.
                // FIXME Inefficient!
                DataPath data_path = new DataPath(value);
                String predicate = data_path.getLastPathStep().toString();
                // Remove the "tuple" token so that only the [...] remains
                predicate = predicate.replaceFirst("tuple", "");
                tuple_array.add(new JsonPrimitive(predicate));
            }
            
            // Stop serializing descendants if the tuple value is a leaf
            if (leaf_values != null && leaf_values.contains(tuple_value)) return;
            
            for (AttributeValueEntry entry : tuple_value)
            {
                Value attribute_value = entry.getValue();
                serializeValue(attribute_value, tuple_array, leaf_values, add_predicate_attribute);
            }
        }
        else if (value instanceof CollectionValue)
        {
            CollectionValue collection_value = (CollectionValue) value;
            
            // Add the collection value array
            JsonArray collection_array = new JsonArray();
            array.add(collection_array);
            
            for (TupleValue tuple_value : collection_value.getTuples())
            {
                serializeValue(tuple_value, collection_array, leaf_values, add_predicate_attribute);
            }
        }
        else if (value instanceof SwitchValue)
        {
            SwitchValue switch_value = (SwitchValue) value;
            
            // Add the switch value array
            JsonArray switch_array = new JsonArray();
            array.add(switch_array);
            
            switch_array.add(new JsonPrimitive(switch_value.getCaseName()));
            TupleValue case_value = (TupleValue) switch_value.getCase();
            serializeValue(case_value, switch_array, leaf_values, add_predicate_attribute);
        }
        else if (value instanceof XhtmlValue)
        {
            XhtmlValue xhtml_value = (XhtmlValue) value;
            
            /*
             * We have to set the default xml namespace to xhtml so that html elements are properly parsed by a browser's DOMParser.
             * We prefix the template element with fesc in order to assign it a different namespace because IE9 and Chrome 26 fail
             * to parse it properly if left in the default xhtml namespace. We assume the xhtml_value is valid.
             */
            String xhtml = "<fesc:template xmlns=\"http://www.w3.org/1999/xhtml\" xmlns:fesc=\"http://forward.ucsd.edu/escape\">";
            xhtml += xhtml_value.getObject();
            xhtml += "</fesc:template>";
            array.add(new JsonPrimitive(xhtml));
        }
        else if (value instanceof BooleanValue)
        {
            BooleanValue boolean_value = (BooleanValue) value;
            
            array.add((boolean_value.getObject()) ? new JsonPrimitive(true) : new JsonPrimitive(false));
        }
        else if (value instanceof NumericValue<?>)
        {
            NumericValue<?> numeric_value = (NumericValue<?>) value;
            array.add(new JsonPrimitive((Number) numeric_value.getObject()));
        }
        else if (value instanceof ScalarValue)
        {
            ScalarValue scalar_value = (ScalarValue) value;
            array.add(new JsonPrimitive(scalar_value.toString()));
        }
        else
        {
            assert (value instanceof NullValue);
            
            array.add(new JsonNull());
        }
    }
}
