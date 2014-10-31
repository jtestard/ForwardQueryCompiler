/**
 * 
 */
package edu.ucsd.forward.data;

import java.util.ArrayList;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.SwitchType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.SwitchValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;

/**
 * Utility class to map a data tree to a schema tree which is "smaller" than the data tree's original schema tree, and remove the
 * umpapped part of the data tree.
 * 
 * @author Kevin
 * 
 */
public final class DataSchemaReduceMapUtil
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DataSchemaReduceMapUtil.class);
    
    /**
     * Private constructor.
     */
    private DataSchemaReduceMapUtil()
    {
    }
    
    /**
     * Maps a data tree to a schema tree which is "smaller" than the data tree's original schema tree, and remove the umpapped part
     * of the data tree.
     * 
     * @param data_tree
     *            the data tree.
     * @param schema_tree
     *            the new schema_tree to map with
     */
    public static void reduceMap(DataTree data_tree, SchemaTree schema_tree)
    {
        reduceMapPrivate(data_tree.getRootValue(), schema_tree.getRootType());
        
        data_tree.setType(schema_tree);
        
        // DataTypeMapUtil.map(dataset, dataset_type);
    }
    
    /**
     * Private implementation.
     * 
     * @param value
     *            the value.
     * @param type
     *            the target type.
     */
    private static void reduceMapPrivate(Value value, Type type)
    {
        if (value instanceof TupleValue)
        {
            assert (type instanceof TupleType);
            
            TupleValue tuple = (TupleValue) value;
            TupleType tuple_type = (TupleType) type;
            
            tuple.setType(tuple_type);
            
            ArrayList<String> data_attrib_names = new ArrayList<String>(tuple.getAttributeNames());
            ArrayList<String> type_attrib_names = new ArrayList<String>(tuple_type.getAttributeNames());
            
            for (String data_attrib_name : data_attrib_names)
            {
                if (type_attrib_names.contains(data_attrib_name))
                {
                    tuple.getAttribute(data_attrib_name).setType(tuple_type.getAttribute(data_attrib_name));
                }
                else
                {
                    tuple.removeAttribute(data_attrib_name);
                }
            }
        }
        else if (value instanceof CollectionValue)
        {
            value.setType(type);
            
            TupleType tuple_type = ((CollectionType) type).getTupleType();
            
            for (Value child : value.getChildren())
            {
                // TODO Optimize here.
                reduceMapPrivate(child, tuple_type);
            }
        }
        else if (value instanceof SwitchValue)
        {
            SwitchValue switch_value = (SwitchValue) value;
            SwitchType switch_type = (SwitchType) type;
            
            switch_value.setType(switch_type);
            reduceMapPrivate(switch_value.getCase(), switch_type.getCase(switch_value.getCaseName()));
        }
    }
}
