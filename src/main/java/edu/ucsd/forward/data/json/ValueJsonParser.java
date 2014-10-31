/**
 * 
 */
package edu.ucsd.forward.data.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.DoubleType;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.LongType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.SwitchType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.XhtmlType;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.DoubleValue;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.LongValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.SwitchValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.data.value.XhtmlValue;

/**
 * Utility class to parse a JSON string into a DataTree.
 * 
 * @author Hongping Lim
 */
public final class ValueJsonParser
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ValueJsonParser.class);
    
    /**
     * Hidden constructor.
     */
    private ValueJsonParser()
    {
    }
    
    /**
     * Parses a JSON string into a data tree.
     * 
     * @param json_string
     *            JSON string
     * @param schema_tree
     *            schema tree
     * @return data tree
     */
    public static DataTree parseDataTree(String json_string, SchemaTree schema_tree)
    {
        return parseDataTree(json_string, schema_tree, true);
    }
    
    /**
     * Parses a JSON string into a data tree.
     * 
     * @param json_string
     *            JSON string
     * @param schema_tree
     *            schema tree
     * @param added_predicate_attribute
     *            whether the serializer added the predicate for collection tuples
     * @return data tree
     */
    public static DataTree parseDataTree(String json_string, SchemaTree schema_tree, boolean added_predicate_attribute)
    {
        JsonArray a = new JsonParser().parse(json_string).getAsJsonArray();
        
        Value root_value = parseValue(a.get(0), schema_tree.getRootType(), added_predicate_attribute);
        DataTree data_tree = new DataTree(root_value);
        return data_tree;
    }
    
    /**
     * Creates a Value from the given JsonElement.
     * 
     * @param o
     *            json element
     * @param type
     *            type
     * @param added_predicate_attribute
     *            whether the serializer added the predicate for collection tuples
     * @return value
     */
    public static Value parseValue(JsonElement o, Type type, boolean added_predicate_attribute)
    {
        return parseValue(o, type, added_predicate_attribute, 0);
    }
    
    /**
     * Creates a Value from the given JsonElement.
     * 
     * @param o
     *            json element
     * @param type
     *            type
     * @param start_index
     *            index to start processing an array from
     * @param added_predicate_attribute
     *            whether the serializer added the predicate for collection tuples
     * @return value
     */
    private static Value parseValue(JsonElement o, Type type, boolean added_predicate_attribute, int start_index)
    {
        if (o.isJsonNull())
        {
            return new NullValue();
        }
        
        if (type instanceof CollectionType)
        {
            CollectionType collection_type = (CollectionType) type;
            CollectionValue collection_value = new CollectionValue();
            assert (o.isJsonArray());
            JsonArray a = o.getAsJsonArray();
            for (JsonElement e : a)
            {
                JsonArray element_array = e.getAsJsonArray();
                // Skip over the key which is at index 0 if necessary
                int index = start_index;
                if (added_predicate_attribute)
                {
                    index++;
                }
                Value child_value = parseValue(element_array, collection_type.getTupleType(), added_predicate_attribute, index);
                collection_value.add((TupleValue) child_value);
            }
            return collection_value;
        }
        else if (type instanceof TupleType)
        {
            TupleType tuple_type = (TupleType) type;
            TupleValue tuple_value = new TupleValue();
            assert (o.isJsonArray());
            JsonArray a = o.getAsJsonArray();
            int i = 0;
            for (String name : tuple_type.getAttributeNames())
            {
                JsonElement e = a.get(start_index + i);
                Value child_value = parseValue(e, tuple_type.getAttribute(name), added_predicate_attribute);
                tuple_value.setAttribute(name, child_value);
                i++;
            }
            return tuple_value;
        }
        else if (type instanceof SwitchType)
        {
            SwitchType switch_type = (SwitchType) type;
            SwitchValue switch_value = new SwitchValue();
            assert (o.isJsonArray());
            JsonArray a = o.getAsJsonArray();
            String case_name = a.get(0).getAsString();
            assert (switch_type.getCase(case_name) != null) : "Case name " + case_name + " is not within the switch type";
            Value child_value = parseValue(a.get(1), ((SwitchType) type).getCase(case_name), added_predicate_attribute);
            switch_value.setCase(case_name, child_value);
            return switch_value;
        }
        else if (type instanceof StringType)
        {
            return new StringValue(o.getAsString());
        }
        else if (type instanceof IntegerType)
        {
            return new IntegerValue(o.getAsInt());
        }
        else if (type instanceof LongType)
        {
            return new LongValue(o.getAsLong());
        }
        else if (type instanceof DoubleType)
        {
            return new DoubleValue(o.getAsDouble());
        }
        else if (type instanceof BooleanType)
        {
            return new BooleanValue(o.getAsBoolean());
        }
        else if (type instanceof XhtmlType)
        {
            return new XhtmlValue(o.getAsString());
        }
        
        throw new UnsupportedOperationException(type.toString());
    }
}
