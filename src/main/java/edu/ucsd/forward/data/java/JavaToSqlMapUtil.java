/**
 * 
 */
package edu.ucsd.forward.data.java;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpSession;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.SwitchType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeConverter;
import edu.ucsd.forward.data.type.TypeException;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.DateValue;
import edu.ucsd.forward.data.value.DecimalValue;
import edu.ucsd.forward.data.value.DoubleValue;
import edu.ucsd.forward.data.value.FloatValue;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.LongValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.SwitchValue;
import edu.ucsd.forward.data.value.TimestampValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.ExceptionMessages;

/**
 * A Java to SQL++ converter that given a SQL++ type t and a Java object reference j, create an SQL++ value v in a best effort but
 * consistent manner.
 * 
 * @author Yupeng
 * 
 */
public final class JavaToSqlMapUtil
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(JavaToSqlMapUtil.class);
    
    /**
     * Hidden constructor.
     */
    private JavaToSqlMapUtil()
    {
        
    }
    
    /**
     * Maps a Java object into a scalar value in a best effort way.
     * 
     * If the object passed in is null, this returns a NullValue.
     * 
     * @param obj
     *            the object to map
     * @return the mapped scalar value.
     * @throws TypeException
     *             if the object cannot be mapped to any scalar value.
     */
    public static Value mapScalar(Object obj) throws TypeException
    {
        ScalarValue result = null;
        if (obj instanceof String) result = new StringValue((String) obj);
        else if (obj instanceof Integer) result = new IntegerValue(((Integer) obj).intValue());
        else if (obj instanceof Long) result = new LongValue(((Long) obj).longValue());
        else if (obj instanceof Double) result = new DoubleValue(((Double) obj).doubleValue());
        else if (obj instanceof Float) result = new FloatValue((Float) obj);
        else if (obj instanceof BigDecimal) result = new DecimalValue(((BigDecimal) obj));
        else if (obj instanceof Boolean) result = new BooleanValue(((Boolean) obj).booleanValue());
        else if (obj instanceof Date) result = new DateValue(((Date) obj));
        else if (obj instanceof Timestamp) result = new TimestampValue(((Timestamp) obj));
        else if (obj == null) return new NullValue();
        
        // Cannot convert object to the raw scalar value
        if (result == null) throw new TypeException(ExceptionMessages.Type.INVALID_JAVA_TYPE_MAPPING, obj.getClass().toString(),
                                                    ScalarType.class.getName());
        
        return result;
    }
    
    /**
     * Maps a Java object to the SQL++ value with respect to the given SQL++ type.
     * 
     * @param type
     *            the target SQL++ type
     * @param obj
     *            the Java object to map.
     * @return the mapped SQL++ value.
     * @throws TypeException
     *             anything wrong happens during mapping
     */
    public static Value map(Type type, Object obj) throws TypeException
    {
        if (type instanceof SchemaTree)
        {
            Value root_value = map(((SchemaTree) type).getRootType(), obj);
            return new DataTree(root_value);
        }
        
        Value value;
        if (obj == null)
        {
            if (type instanceof CollectionType) value = new CollectionValue();
            else value = new NullValue();
            return value;
        }
        
        // Convert the Java object with the best-effort mapping
        value = convert(type, obj);
        for (Type child_type : type.getChildren())
        {
            // Get the relative path step from parent to child
            SchemaPath path = new SchemaPath(type, child_type);
            // Navigate in the Java object to get the child object with respect to the given path
            List<Object> list = navigate(obj, type, path);
            for (Object child_obj : list)
            {
                Value child_value = map(child_type, child_obj);
                // A check for switch value that there is no existing case
                if (obj instanceof SwitchValue)
                {
                    SwitchValue switch_value = ((SwitchValue) obj);
                    if (switch_value.getCase() != null && !(switch_value.getCase() instanceof NullValue))
                    {
                        // FIXME: What if an empty collection value?
                        throw new TypeException(ExceptionMessages.Type.MORE_THAN_ONE_SWITCH_CASE, obj.getClass().toString());
                    }
                }
                // Attach the child value to the parent
                ValueUtil.attach(value, path.getString(), child_value);
            }
        }
        return value;
    }
    
    /**
     * Converts a Java object to the SQL++ value with respect to the given SQL++ type, excluding the children of the object.
     * 
     * @param type
     *            the target SQL++ type
     * @param obj
     *            the Java object to convert.
     * @return the mapped SQL++ value.
     * @throws TypeException
     *             when the object cannot be converted.
     */
    private static Value convert(Type type, Object obj) throws TypeException
    {
        if (type instanceof ScalarType)
        {
            return convertScalar((ScalarType) type, obj);
        }
        else if (type instanceof TupleType)
        {
            // Always return a tuple value
            return new TupleValue();
        }
        else if (type instanceof SwitchType)
        {
            return new SwitchValue();
        }
        else if (type instanceof CollectionType)
        {
            return new CollectionValue();
        }
        throw new TypeException(ExceptionMessages.Type.INVALID_JAVA_TYPE_MAPPING, obj.getClass().toString(), type.toString());
    }
    
    /**
     * Converts a Java object to the target scalar value with the respect to the scalar type.
     * 
     * @param type
     *            the target scalar type.
     * @param obj
     *            the Java object to convert.
     * @return the mapped scalar value.
     * @throws TypeException
     *             when the object cannot be converted.
     */
    private static ScalarValue convertScalar(ScalarType type, Object obj) throws TypeException
    {
        ScalarValue raw = null;
        
        if (obj instanceof String) raw = new StringValue((String) obj);
        else if (obj instanceof Integer) raw = new IntegerValue(((Integer) obj).intValue());
        else if (obj instanceof Long) raw = new LongValue(((Long) obj).longValue());
        else if (obj instanceof Double) raw = new DoubleValue(((Double) obj).doubleValue());
        else if (obj instanceof Float) raw = new FloatValue((Float) obj);
        else if (obj instanceof BigDecimal) raw = new DecimalValue(((BigDecimal) obj));
        else if (obj instanceof Boolean) raw = new BooleanValue(((Boolean) obj).booleanValue());
        else if (obj instanceof Date) raw = new DateValue(((Date) obj));
        else if (obj instanceof Timestamp) raw = new TimestampValue(((Timestamp) obj));
        
        // Cannot convert object to the raw scalar value
        if (raw == null) throw new TypeException(ExceptionMessages.Type.INVALID_JAVA_TYPE_MAPPING, obj.getClass().toString(),
                                                 type.toString());
        
        // Try convert the raw value to the target type
        ScalarValue value = (ScalarValue) TypeConverter.getInstance().convert(raw, type);
        return value;
    }
    
    /**
     * Navigates into the object with respect to the given SQL++ type and schema path.
     * 
     * @param obj
     *            the Java object
     * @param type
     *            the type corresponding to the object
     * @param path
     *            the path to probe
     * @return the objects navigated into.
     * @throws TypeException
     *             when the object cannot be converted.
     */
    @SuppressWarnings("unchecked")
    private static List<Object> navigate(Object obj, Type type, SchemaPath path) throws TypeException
    {
        List<Object> list = new ArrayList<Object>();
        if (type instanceof TupleType)
        {
            String attribute_name = path.getLastPathStep();
            Object child_obj = navigateTuple(obj, type, attribute_name);
            list.add(child_obj);
        }
        else if (type instanceof SwitchType)
        {
            String case_name = path.getLastPathStep();
            // Using the same navigation as in the tuple
            Object child_obj = navigateSwitch(obj, type, case_name);
            list.add(child_obj);
        }
        else if (type instanceof CollectionType)
        {
            if (obj instanceof Map)
            {
                for (Object entry : ((Map) obj).entrySet())
                {
                    MapEntry map_entry = new MapEntry(((Entry) entry).getKey(), ((Entry) entry).getValue());
                    list.add(map_entry);
                }
            }
            else if (obj instanceof Iterable)
            {
                for (Object child : (Iterable) obj)
                {
                    list.add(child);
                }
            }
            else throw new TypeException(ExceptionMessages.Type.INVALID_JAVA_TYPE_MAPPING, obj.getClass().toString(),
                                         type.toString());
        }
        
        return list;
    }
    
    @SuppressWarnings("unchecked")
    private static Object navigateSwitch(Object obj, Type type, String attribute_name) throws TypeException
    {
        if (obj instanceof Map) return ((Map) obj).get(attribute_name);
        else if (obj instanceof HttpSession) return ((HttpSession) obj).getAttribute(attribute_name);
        else
        {
            String expected_getter_name = "get" + attribute_name.substring(0, 1).toUpperCase()
                    + attribute_name.substring(1).toLowerCase();
            for (Method method : obj.getClass().getDeclaredMethods())
            {
                if (method.getName().equalsIgnoreCase(expected_getter_name))
                {
                    // Try calling method getAttributeName() from obj.
                    try
                    {
                        return method.invoke(obj);
                    }
                    catch (Exception e)
                    {
                        throw new TypeException(ExceptionMessages.Type.ERROR_INVOKING_GETTER, expected_getter_name,
                                                obj.getClass().toString());
                    }
                }
            }
        }
        return null;
    }
    
    @SuppressWarnings("unchecked")
    private static Object navigateTuple(Object obj, Type type, String attribute_name) throws TypeException
    {
        if (obj instanceof Map) return ((Map) obj).get(attribute_name);
        else if (obj instanceof HttpSession) return ((HttpSession) obj).getAttribute(attribute_name);
        else
        {
            String expected_getter_name = "get" + attribute_name.substring(0, 1).toUpperCase()
                    + attribute_name.substring(1).toLowerCase();
            for (Method method : obj.getClass().getDeclaredMethods())
            {
                if (method.getName().equalsIgnoreCase(expected_getter_name))
                {
                    // Try calling method getAttributeName() from obj.
                    try
                    {
                        return method.invoke(obj);
                    }
                    catch (Exception e)
                    {
                        throw new TypeException(ExceptionMessages.Type.ERROR_INVOKING_GETTER, expected_getter_name,
                                                obj.getClass().toString());
                    }
                }
            }
            throw new TypeException(ExceptionMessages.Type.NON_EXISTING_GETTER, expected_getter_name, obj.getClass().toString());
        }
    }
}
