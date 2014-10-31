/**
 * 
 */
package edu.ucsd.forward.data.json;

import java.math.BigDecimal;

import org.apache.commons.lang.StringUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.DecimalType;
import edu.ucsd.forward.data.type.DoubleType;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.LongType;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeException;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DecimalValue;
import edu.ucsd.forward.data.value.DoubleValue;
import edu.ucsd.forward.data.value.FloatValue;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.JsonValue;
import edu.ucsd.forward.data.value.LongValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.NumericValue;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.ExceptionMessages;

/**
 * Converts JSON to FORWARD Values.
 * 
 * @author Christopher Rebert
 */
public final class JsonToValueConverter
{
    private static final String UNDERSCORE = "_";
    
    /**
     * Converts a JSON value to other FORWARD value.
     * 
     * @param value
     *            the json value
     * @param type
     *            the FORWARD type to convert the element to
     * @return a FORWARD Value corresponding to the given element
     * @throws IllegalStateException
     * @throws UnsupportedOperationException
     */
    public static Value convert(JsonValue value, Type type) throws IllegalStateException, UnsupportedOperationException
    {
        return convert(value.getElement(), type);
    }
    
    /**
     * Converts a JSON element to a FORWARD value of specified type. It does so loosely: JSON objects may have members beyond those
     * of the specified FORWARD type; these extra members will be ignored. Assumes JSON member names and FORWARD attribute names
     * either match exactly, or follow (respectively) the camelCase and underscore_separated naming conventions (which it converts
     * between).
     * 
     * @param elem
     *            the JSON element to convert
     * @param type
     *            the FORWARD type to convert the element to
     * @return a FORWARD Value corresponding to the given element
     * @throws IllegalStateException
     *             when a JSON element does not match its FORWARD type
     * @throws UnsupportedOperationException
     *             when type is SwitchType or another unsupported FORWARD type
     */
    public static Value convert(JsonElement elem, Type type) throws IllegalStateException, UnsupportedOperationException
    {
        if (type instanceof CollectionType)
        {
            CollectionType collection_type = (CollectionType) type;
            TupleType item_type = collection_type.getTupleType();
            
            JsonArray array = elem.getAsJsonArray();
            CollectionValue collection = new CollectionValue();
            for (JsonElement item : array)
            {
                collection.add((TupleValue) convert(item, item_type));
            }
            return collection;
        }
        else if (type instanceof TupleType)
        {
            JsonObject obj = elem.getAsJsonObject();
            TupleType tuple_type = (TupleType) type;
            TupleValue tuple = new TupleValue();
            for (String attr_name : tuple_type.getAttributeNames())
            {
                Type attr_type = tuple_type.getAttribute(attr_name);
                JsonElement member = obj.get(attr_name);
                if (member == null)
                {
                    member = obj.get(underscoreSeparatedToCamelCase(attr_name));
                }
                if (member == null)
                {
                    throw new IllegalStateException("No corresponding member for '" + attr_name + "' in given JSON object: " + obj);
                }
                Value attr_value = convert(member, attr_type);
                tuple.setAttribute(attr_name, attr_value);
            }
            return tuple;
        }
        else if (elem.isJsonNull())
        {
            return new NullValue(type.getClass());
        }
        else if (type instanceof StringType)
        {
            return new StringValue(elem.getAsString());
        }
        else if (type instanceof IntegerType)
        {
            return new IntegerValue(elem.getAsInt());
        }
        else if (type instanceof LongType)
        {
            return new LongValue(elem.getAsLong());
        }
        else if (type instanceof DoubleType)
        {
            return new DoubleValue(elem.getAsDouble());
        }
        else if (type instanceof DecimalType)
        {
            return new DecimalValue(elem.getAsBigDecimal());
        }
        else if (type instanceof BooleanType)
        {
            return new BooleanValue(elem.getAsBoolean());
        }
        else
        {
            throw new UnsupportedOperationException(type.toString());
        }
    }
    
    /**
     * Converts an underscore_separated identifier to a camelCased one. Assumes identifiers never have trailing underscores.
     * 
     * @param underscore_separated
     *            an underscore_separated identifier
     * @return the same identifier under the camelCase naming convention
     */
    private static String underscoreSeparatedToCamelCase(String underscore_separated)
    {
        String[] words = underscore_separated.split(UNDERSCORE);
        for (int i = 1; i < words.length; i++)
        {
            String word = words[i];
            String capitalized_word = "" + Character.toUpperCase(word.charAt(0));
            if (word.length() > 1)
            {
                capitalized_word += word.substring(1);
            }
            words[i] = capitalized_word;
        }
        return StringUtils.join(words);
    }
    
    /**
     * Not instantiatable.
     */
    private JsonToValueConverter()
    {
    }
    
    /**
     * Maps a JSON value into a scalar value in a best effort way. If the input is JSON null, then returns null value.
     * 
     * @param value
     *            the object to map
     * @return the mapped scalar value.
     * @throws TypeException
     *             if the object cannot be mapped to any scalar value.
     */
    public static Value mapScalar(JsonValue value) throws TypeException
    {
        return mapScalar(value.getElement());
    }
    
    /**
     * Maps a JSON element into a scalar value in a best effort way.
     * 
     * @param element
     *            the object to map
     * @return the mapped scalar value.
     * @throws TypeException
     *             if the object cannot be mapped to any scalar value.
     */
    public static Value mapScalar(JsonElement element) throws TypeException
    {
        if (element instanceof JsonNull) return new NullValue();
        if (!element.isJsonPrimitive()) throw new TypeException(ExceptionMessages.Type.INVALID_JSON_TYPE_MAPPING,
                                                                element.toString());
        JsonPrimitive primitive = element.getAsJsonPrimitive();
        if (primitive.isBoolean()) return new BooleanValue(primitive.getAsBoolean());
        else if (primitive.isString()) return new StringValue(primitive.getAsString());
        else if (primitive.isNumber()) return coerse(primitive.getAsNumber().toString());
        else throw new TypeException(ExceptionMessages.Type.INVALID_JSON_TYPE_MAPPING, element.toString());
        
    }
    
    /**
     * This method coerces the input value into a low-precision type while preserving the initial precision.
     * 
     * @param literal
     *            the literal to coerce.
     * @return a coerced value.
     */
    private static NumericValue<?> coerse(String literal)
    {
        BigDecimal decimal_obj = new BigDecimal(literal);
        
        int int_value = decimal_obj.intValue();
        if (decimal_obj.compareTo(new BigDecimal(int_value)) == 0)
        {
            return new IntegerValue(int_value);
        }
        
        long long_value = decimal_obj.longValue();
        if (decimal_obj.compareTo(new BigDecimal(long_value)) == 0)
        {
            return new LongValue(long_value);
        }
        
        float float_value = decimal_obj.floatValue();
        if (decimal_obj.compareTo(new BigDecimal(float_value)) == 0)
        {
            return new FloatValue(float_value);
        }
        
        double double_value = decimal_obj.doubleValue();
        if (decimal_obj.compareTo(new BigDecimal(double_value)) == 0)
        {
            return new DoubleValue(double_value);
        }
        
        return new DecimalValue(decimal_obj);
    }
}
