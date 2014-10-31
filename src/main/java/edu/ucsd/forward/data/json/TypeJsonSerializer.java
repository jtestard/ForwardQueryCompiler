/**
 * 
 */
package edu.ucsd.forward.data.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

import edu.ucsd.app2you.util.identity.IdentityHashSet;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.SwitchType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.TupleType.AttributeEntry;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeEnum;

/**
 * Utility class to serialize a type to a JSON object.
 * 
 * The format used is as follows:
 * 
 * <schema_tree> ::- [ <type> ]
 * 
 * <type> ::- <tuple_type> | <collection_type> | <switch_type> | <scalar_type>
 * 
 * <tuple_type> ::- "tuple", [ ["attr_name", <type>], ... ]
 * 
 * <collection_type> ::- "collection", <tuple_type>
 * 
 * <switch_type> ::- "switch", [ ["case_name", "tuple", [ <type>, ... ], ["case_name", "tuple", [ <type>, ... ] ... ]
 * 
 * <scalar_type> ::- "scalar_type_name"
 * 
 * @author Hongping Lim
 * @author Michalis Petropoulos
 * 
 */
public final class TypeJsonSerializer
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TypeJsonSerializer.class);
    
    /**
     * Hidden constructor.
     */
    private TypeJsonSerializer()
    {
    }
    
    /**
     * Serializes a schema tree to a JSON array. Constraints are not serialized.
     * 
     * @param schema_tree
     *            a schema tree to serialize.
     * @return a JSON array.
     */
    public static JsonArray serializeSchemaTree(SchemaTree schema_tree)
    {
        JsonArray array = new JsonArray();
        
        serializeType(schema_tree.getRootType(), array, null);
        
        return array;
    }
    
    /**
     * Serializes a type and adds it to a JSON array. The given set of leaf types indicate where to stop serializing: the leaf
     * (tuple) types will be serialized, but their descendants will not.
     * 
     * @param type
     *            the type to serialize.
     * @param array
     *            the containing JSON array.
     * @param leaf_types
     *            the set of leaf types.
     */
    public static void serializeType(Type type, JsonArray array, IdentityHashSet<TupleType> leaf_types)
    {
        assert (array != null);
        assert (type != null);
        
        array.add(new JsonPrimitive(TypeEnum.getName(type)));
        
        // Recurse
        if (type instanceof TupleType)
        {
            TupleType tuple_type = (TupleType) type;
            
            // Add the tuple type array
            JsonArray tuple_array = new JsonArray();
            array.add(tuple_array);
            
            // Stop serializing descendants if the tuple type is a leaf
            if (leaf_types != null && leaf_types.contains(tuple_type)) return;
            
            for (AttributeEntry entry : tuple_type)
            {
                // Add the attribute type array
                JsonArray attr_array = new JsonArray();
                tuple_array.add(attr_array);
                
                attr_array.add(new JsonPrimitive(entry.getName()));
                
                serializeType(entry.getType(), attr_array, leaf_types);
            }
        }
        else if (type instanceof CollectionType)
        {
            CollectionType collection_type = (CollectionType) type;
            serializeType(collection_type.getTupleType(), array, leaf_types);
        }
        else
        {
            assert (type instanceof ScalarType);
            // Nothing to do
        }
    }
    
}
