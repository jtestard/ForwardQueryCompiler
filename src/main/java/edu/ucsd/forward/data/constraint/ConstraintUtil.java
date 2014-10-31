/**
 * 
 */
package edu.ucsd.forward.data.constraint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.UnknownType;

/**
 * Utility methods for constraints.
 * 
 * @author Yupeng
 * 
 */
public final class ConstraintUtil
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ConstraintUtil.class);
    
    /**
     * Private constructor.
     */
    private ConstraintUtil()
    {
        
    }
    
    /**
     * Gets the non-null constraint of the given type. If there is not one, return <code>null</code>
     * 
     * @param type
     *            the type
     * @return the non-null constraint of the given type. If there is not one, return <code>null</code>
     */
    public static NonNullConstraint getNonNullConstraint(Type type)
    {
        assert type != null;
        if (type instanceof CollectionType) return null;
        
        List<Constraint> constraints = type.getConstraints();
        for (Constraint constraint : constraints)
        {
            if (!(constraint instanceof NonNullConstraint)) continue;
            
            NonNullConstraint c = (NonNullConstraint) constraint;
            if (c.getAttribute() != type) continue;
            
            return c;
        }
        return null;
    }
    
    /**
     * Gets the auto increment constraint of the given scalar type. If there is not one, return <code>null</code>
     * 
     * @param scalar_type
     *            the scalar type
     * @return the auto increment constraint of the given type. If there is not one, return <code>null</code>
     */
    public static AutoIncrementConstraint getAutoIncrementConstraint(ScalarType scalar_type)
    {
        assert scalar_type != null;
        
        List<Constraint> constraints = scalar_type.getConstraints();
        for (Constraint constraint : constraints)
        {
            if (!(constraint instanceof AutoIncrementConstraint)) continue;
            
            AutoIncrementConstraint c = (AutoIncrementConstraint) constraint;
            if (c.getAttribute() != scalar_type) continue;
            return c;
        }
        return null;
    }
    
    /**
     * Adds primary key constraint to collection. The constructor for the primary key constraint only requires the schema path from
     * the collection to the attribute, which is extracted from the key types themselves. Therefore, we temporarily add the types to
     * the collection tuple (if they don't exist), set the key constraints, then remove the types that were added if keep_types is
     * false. In the case when we encounter an unknown type, we replace it with a string type, then restore it when we are done.
     * 
     * Note: if this collection is to be serialized (TypeXmlSerializer), which is most likely the case, removing the types added for
     * the constraints will cause the serializer to fail. The serializer requires the types to be present when serializing a
     * constraint.
     * 
     * @param collection
     *            The collection to add the key constraint to.
     * @param keys
     *            the keys to add.
     * @param keep_types
     *            Whether we want to keep the types in the collection after we have added the constraint.
     */
    public static void addKeyConstraint(CollectionType collection, List<String> keys, boolean keep_types)
    {
        assert (collection != null);
        if (keys == null || keys.isEmpty()) return;
        
        Map<String, Type> replace_map = new HashMap<String, Type>();
        HashSet<String> keys_copy = new HashSet<String>(keys);
        TupleType collection_tuple = collection.getTupleType();
        List<Type> attribute_keys = new ArrayList<Type>();
        
        for (String key : keys)
        {
            Type key_type = collection_tuple.getAttribute(key);
            if (key_type instanceof UnknownType)
            {
                keys_copy.remove(key);
                replace_map.put(key, key_type);
                key_type = new StringType();
            }
            else if (key_type != null)
            {
                keys_copy.remove(key);
            }
            else
            {
                key_type = new StringType();
            }
            
            collection_tuple.setAttribute(key, key_type);
            
            attribute_keys.add(key_type);
        }
        
        // Set the primary key constraint.
        new LocalPrimaryKeyConstraint(collection, attribute_keys);
        
        // Remove the added attributes from the collection
        if (!keep_types)
        {
            for (String key : keys_copy)
            {
                collection_tuple.removeAttribute(key);
            }
        }
        
        // Replace types
        for (Entry<String, Type> entry : replace_map.entrySet())
        {
            collection_tuple.setAttribute(entry.getKey(), entry.getValue());
        }
    }
}
