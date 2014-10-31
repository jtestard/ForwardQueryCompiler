/**
 * 
 */
package edu.ucsd.forward.data.index;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.DataPath;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.TupleValue;

/**
 * Utility methods for indices.
 * 
 * @author Yupeng
 * 
 */
public final class IndexUtil
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(IndexUtil.class);
    
    /**
     * Private constructor.
     */
    private IndexUtil()
    {
        
    }
    
    /**
     * Gets all the index declarations in a given schema tree.
     * 
     * @param schema
     *            the schema tree
     * @return all the index declarations in a given schema tree.
     */
    public static List<IndexDeclaration> getIndexDeclarations(SchemaTree schema)
    {
        List<IndexDeclaration> indices = new ArrayList<IndexDeclaration>();
        for (CollectionType coll_type : schema.getDescendants(CollectionType.class))
        {
            indices.addAll(coll_type.getIndexDeclarations());
        }
        return indices;
    }
    
    /**
     * Gets the index declaration if the given type is key of the index.
     * 
     * @param type
     *            the given type to check.
     * @return the index declaration if the given type is key of it, null otherwise.
     */
    public static IndexDeclaration getIndexDeclaration(Type type)
    {
        // Fail fast
        if (!(type instanceof ScalarType)) return null;
        if (type.getSchemaTree() == null) return null;
        for (IndexDeclaration declaration : getIndexDeclarations(type.getSchemaTree()))
        {
            if (declaration.getKeys().contains(type)) return declaration;
        }
        return null;
    }
    
    /**
     * Adds a tuple to the indices of a given collection.
     * 
     * @param collection
     *            the collection
     * @param tuple
     *            the tuple to add.
     */
    public static void addToIndex(CollectionValue collection, TupleValue tuple)
    {
        // The collection does not belong to a data tree.
        if (collection.getDataTree() == null) return;
        
        DataTree data_tree = collection.getDataTree();
        // FIXME: only operation on the root collection supported.
        if (data_tree.getRootValue() != collection) return;
        
        DataPath data_path = new DataPath(collection);
        List<Index> indices = data_tree.getIndices(data_path.toSchemaPath());
        for (Index index : indices)
        {
            index.insert(tuple);
        }
    }
    
    /**
     * Delete a tuple from the indices of a given collection.
     * 
     * @param collection
     *            the collection.
     * @param tuple
     *            the tuple to remove.
     */
    public static void removeFromIndex(CollectionValue collection, TupleValue tuple)
    {
        // The collection does not belong to a data tree.
        if (collection.getDataTree() == null) return;
        
        DataTree data_tree = collection.getDataTree();
        // FIXME: only operation on the root collection supported.
        if (data_tree.getRootValue() != collection) return;
        
        DataPath data_path = new DataPath(collection);
        List<Index> indices = data_tree.getIndices(data_path.toSchemaPath());
        for (Index index : indices)
        {
            index.delete(tuple);
        }
    }
}
