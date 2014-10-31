/**
 * 
 */
package edu.ucsd.forward.data.constraint;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.DataPath;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;

/**
 * Utility methods to retrieve primary key attributes.
 * 
 * @author Kian Win
 * @author Michalis Petropoulos
 * 
 */
public final class PrimaryKeyUtil
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(PrimaryKeyUtil.class);
    
    /**
     * Private constructor.
     */
    private PrimaryKeyUtil()
    {
        
    }
    
    /**
     * Returns the global primary key attributes of a collection type.
     * 
     * @param collection_type
     *            the collection type.
     * @return the global primary key attributes.
     */
    public static List<ScalarType> getGlobalPrimaryKeyTypes(CollectionType collection_type)
    {
        assert (collection_type != null);
        assert (collection_type.hasLocalPrimaryKeyConstraint());
        
        List<ScalarType> local_primary_key_types = collection_type.getLocalPrimaryKeyConstraint().getAttributes();
        
        CollectionType parent_collection_type = collection_type.getClosestAncestor(CollectionType.class);
        if (parent_collection_type == null)
        {
            // Top collection type - simply use the local primary key attributes
            return local_primary_key_types;
        }
        else
        {
            // Nested collection type - concatenate the parent key attributes with the local primary key attributes
            List<ScalarType> parent_key = getGlobalPrimaryKeyTypes(parent_collection_type);
            
            List<ScalarType> list = new ArrayList<ScalarType>();
            list.addAll(parent_key);
            list.addAll(local_primary_key_types);
            return list;
        }
    }
    
    /**
     * Returns the local primary key attribute values of a collection's child tuple.
     * 
     * @param tuple
     *            the child tuple of a collection.
     * @return the local primary key attribute values.
     */
    public static List<ScalarValue> getLocalPrimaryKeyValues(TupleValue tuple)
    {
        assert (tuple.getParent() instanceof CollectionValue);
        
        // Too expensive to check during production
        // assert (tuple.getDataTree().isTypeConsistent());
        
        CollectionValue collection = (CollectionValue) tuple.getParent();
        CollectionType collection_type = collection.getType();
        assert (collection_type.hasLocalPrimaryKeyConstraint());
        
        return getLocalPrimaryKeyValues(tuple, collection_type.getLocalPrimaryKeyConstraint());
    }
    
    /**
     * Returns the local primary key attribute values of a collection's child tuple. This one does not require the value to have
     * associated type.
     * 
     * @param tuple
     *            the child tuple of a collection.
     * @param constraint
     *            the local primary constraint of the tuple's parent collection
     * @return the local primary key attribute values.
     */
    public static List<ScalarValue> getLocalPrimaryKeyValues(TupleValue tuple, LocalPrimaryKeyConstraint constraint)
    {
        assert (constraint != null);
        
        // Too expensive to check during production
        // assert (tuple.getDataTree().isTypeConsistent());
        
        List<ScalarValue> local_primary_key_values = new ArrayList<ScalarValue>();
        
        for (SchemaPath schema_path : constraint.getAttributesSchemaPaths())
        {
            // Get the relative path after the collection's "tuple" part of the path
            // E.g. "/my_proposals/tuple/id" should provide "id" as the relative path
            SchemaPath path = schema_path.relative(1);
            DataPath data_path = path.toDataPath();
            Value value = data_path.find(tuple);
            
            // FIXME Throw a runtime exception
            
            //FIXME HACK to ackomodate null primary keys
            
            assert (value instanceof ScalarValue);
            
            local_primary_key_values.add((ScalarValue) value);
        }
        
        return local_primary_key_values;
    }
    
    /**
     * Returns the global primary key attribute values of a collection's child tuple. The current implementation is INEFFICIENT.
     * 
     * @param tuple
     *            the child tuple of a collection
     * @return the global primary key attribute values
     */
    public static List<ScalarValue> getGlobalPrimaryKeyValues(TupleValue tuple)
    {
        assert (tuple != null);
        Value parent = tuple.getParent();
        
        assert (parent instanceof CollectionValue);
        
        CollectionValue parent_collection = (CollectionValue) parent;
        
        List<ScalarValue> local_primary_key_values = getLocalPrimaryKeyValues(tuple);
        
        TupleValue parent_collection_tuple_value = null;
        
        {
            Value c = parent_collection;
            while (c != null && !(c.getParent() instanceof CollectionValue))
            {
                c = c.getParent();
            }
            parent_collection_tuple_value = (TupleValue) c;
        }
        
        if (parent_collection_tuple_value == null)
        {
            return local_primary_key_values;
        }
        else
        {
            List<ScalarValue> parent_key = getGlobalPrimaryKeyValues(parent_collection_tuple_value);
            
            parent_key.addAll(local_primary_key_values);
            
            return parent_key;
        }
    }
    
    /**
     * Checks if the given type is the primary key of a collection. Tolerate the case that primary key constraint does not exist.
     * 
     * @param type
     *            the type
     * @return <code>true</code> if the primary key constraint exists and the given type is the primary key of a collection;
     *         <code>false</code> otherwise.
     */
    public static boolean isPrimaryKeyType(Type type)
    {
        if (!(type instanceof ScalarType)) return false;
        CollectionType collection_type = type.getClosestAncestor(CollectionType.class);
        if (collection_type == null) return false;
        
        for (CollectionType ancestor : collection_type.getAncestorsAndSelf(CollectionType.class))
        {
            if (!ancestor.hasLocalPrimaryKeyConstraint()) return false;
        }
        
        for (ScalarType key_type : PrimaryKeyUtil.getGlobalPrimaryKeyTypes(collection_type))
        {
            if (key_type == type) return true;
        }
        return false;
    }
    
}
