/**
 * 
 */
package edu.ucsd.forward.data;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.JsonType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.SwitchType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TupleType.AttributeEntry;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.JsonValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.SwitchValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;

/**
 * Utility class to map values to types. An attribute name that occurs in the tuple type but is missing in the tuple is assumed to
 * be a null value.
 * 
 * @author Kian Win
 * 
 */
public final class ValueTypeMapUtil
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ValueTypeMapUtil.class);
    
    /**
     * Private constructor.
     * 
     */
    private ValueTypeMapUtil()
    {
        
    }
    
    /**
     * Maps a data to a type. The data must be compatible with the type.
     * 
     * @param value
     *            the value.
     * @param type
     *            the type.
     */
    public static void map(Value value, Type type)
    {
        // Ugly proxying code
        
        if (value instanceof DataTree)
        {
            assert (type instanceof SchemaTree);
            map((DataTree) value, (SchemaTree) type);
        }
        else if (value instanceof CollectionValue)
        {
            assert (type instanceof CollectionType);
            map((CollectionValue) value, (CollectionType) type);
        }
        else if (value instanceof TupleValue)
        {
            assert (type instanceof TupleType);
            map((TupleValue) value, (TupleType) type);
        }
        else if (value instanceof SwitchValue)
        {
            assert (type instanceof SwitchType);
            map((SwitchValue) value, (SwitchType) type);
        }
        else if (value instanceof ScalarValue)
        {
            assert (type instanceof ScalarType);
            map((ScalarValue) value, (ScalarType) type);
        }
        else if (value instanceof JsonValue)
        {
            assert type instanceof JsonType;
            setType(value, type);
        }
        else
        {
            assert (value instanceof NullValue);
            map((NullValue) value, (Type) type);
        }
    }
    
    /**
     * Maps a dataset to a dataset type.
     * 
     * @param dataset
     *            - the dataset.
     * @param dataset_type
     *            - the dataset type.
     */
    public static void map(DataTree dataset, SchemaTree dataset_type)
    {
        setType(dataset, dataset_type);
        map(dataset.getRootValue(), dataset_type.getRootType());
        
        // Check only type consistency, but not constraint consistency
        dataset.checkTypeConsistent();
    }
    
    /**
     * Maps a tuple to a tuple type. An attribute name that occurs in the tuple type but is missing in the tuple is assumed to be a
     * null value.
     * 
     * @param tuple
     *            - the tuple.
     * @param tuple_type
     *            - the tuple_type.
     */
    public static void map(TupleValue tuple, TupleType tuple_type)
    {
        setType(tuple, tuple_type);
        
        // Use the attribute names of the tuple type
        for (AttributeEntry entry : tuple_type)
        {
            String attribute_name = entry.getName();
            Value value = tuple.getAttribute(attribute_name);
            Type value_type = entry.getType();
            
            // An attribute name that occurs in the tuple type but is missing in the tuple is assumed to be a null value, unless it
            // is a scalar type and has a default value.
            if (value == null)
            {
                if (value_type instanceof CollectionType) value = new CollectionValue();
                else if (value_type instanceof SwitchType) value = new NullValue(SwitchType.class);
                else if (value_type instanceof TupleType) value = new NullValue(TupleType.class);
                else
                {
                    assert (value_type instanceof ScalarType);
                    ScalarType scalar_type = (ScalarType) value_type;
                    value = ValueUtil.cloneNoParentNoType(scalar_type.getDefaultValue());
                }
                tuple.setAttribute(attribute_name, value);
            }
            assert (value_type != null);
            
            // remove the attributes that do not have matching type
            for (String name : tuple.getAttributeNames())
            {
                if (!tuple_type.hasAttribute(name))
                {
                    tuple.removeAttribute(name);
                }
            }
            
            map(value, value_type);
        }
        
        // Ensure that the order of attributes in the value matches the order in the type
        List<String> value_attrs = new ArrayList<String>(tuple.getAttributeNames());
        List<String> type_attrs = new ArrayList<String>(tuple_type.getAttributeNames());
        if (!type_attrs.equals(value_attrs))
        {
            Map<String, Value> map = new LinkedHashMap<String, Value>();
            for (String name : type_attrs)
            {
                map.put(name, tuple.removeAttribute(name));
            }
            for (String name : type_attrs)
            {
                tuple.setAttribute(name, map.get(name));
            }
        }
        
        // FIXME: Temporally disabled the type/data match checking, this allows the query rewriting in query processor to output
        // auxiliary annotation attributes
        // List<String> tuple_attribute_names = new ArrayList<String>(tuple.getAttributeNames());
        // List<String> tuple_type_attribute_names = new ArrayList<String>(tuple_type.getAttributeNames());
        // assert (tuple_attribute_names.equals(tuple_type_attribute_names)) :
        // "Attributes in tuple and type do not match for mapping";
    }
    
    /**
     * Maps a relation to a relation type.
     * 
     * @param relation
     *            - the relation.
     * @param relation_type
     *            - the relation type.
     */
    private static void map(CollectionValue relation, CollectionType relation_type)
    {
        setType(relation, relation_type);
        Type child_type = relation_type.getChildrenType();
        for (Value child : relation.getChildren())
        {
            map(child, child_type);
        }
    }
    
    /**
     * Maps a switch value to a switch type.
     * 
     * @param switch_value
     *            - the switch value.
     * @param switch_type
     *            - the switch type.
     */
    private static void map(SwitchValue switch_value, SwitchType switch_type)
    {
        assert (switch_type.getCaseNames().contains(switch_value.getCaseName())) : "Selected case of switch value is not contained in type";
        
        setType(switch_value, switch_type);
        
        Type case_type = switch_type.getCase(switch_value.getCaseName());
        map(switch_value.getCase(), case_type);
    }
    
    /**
     * Maps a scalar value to a scalar type.
     * 
     * @param scalar_value
     *            - the scalar value.
     * @param scalar_type
     *            - the scalar type.
     */
    private static void map(ScalarValue scalar_value, ScalarType scalar_type)
    {
        setType(scalar_value, scalar_type);
    }
    
    /**
     * Maps a null value to a type.
     * 
     * @param null_value
     *            - the null value.
     * @param value_type
     *            - any value type.
     */
    private static void map(NullValue null_value, Type value_type)
    {
        setType(null_value, value_type);
    }
    
    /**
     * Sets the type for a data instance.
     * 
     * @param value
     *            the value.
     * @param type
     *            the type.
     */
    private static void setType(Value value, Type type)
    {
        assert (value != null) : "Data must be non-null";
        assert (type != null) : "Type must be non-null";
        
        Type old_type = value.getType();
        if (old_type != null)
        {
            // Kevin: I do not understand this assertion. We cannot change the type of a dataset?
            
            // If there is already a type set, it should be the identical instance we are trying to set to
            assert (old_type == type);
        }
        else
        {
            value.setType(type);
        }
    }
    
}
