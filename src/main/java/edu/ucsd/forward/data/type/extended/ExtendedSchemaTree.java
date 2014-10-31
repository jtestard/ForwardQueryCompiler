/**
 * 
 */
package edu.ucsd.forward.data.type.extended;

import java.util.ArrayList;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.constraint.Constraint;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.SwitchType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TupleType.AttributeEntry;
import edu.ucsd.forward.exception.UncheckedException;

/**
 * An extended schema tree.
 * 
 * @author Kian Win Ong
 * 
 */
public class ExtendedSchemaTree extends SchemaTree
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ExtendedSchemaTree.class);
    
    /**
     * Constructs the extended schema tree.
     * 
     * @param root_type
     *            the root type.
     */
    public ExtendedSchemaTree(Type root_type)
    {
        super(root_type);
    }
    
    /**
     * Returns true if there is no AnyScalarType found in the tree.
     * 
     * @return
     */
    public boolean isSchemaTree()
    {
        return (!hasAnyScalarType(getRootType()));
    }
    
    /**
     * Creates a new schema tree, attaching a copy of the current data to it. Callers should call isSchemaTree first to ensure there
     * is no AnyScalarType in the tree.
     * 
     * @return schema tree
     */
    public SchemaTree toSchemaTree()
    {
        SchemaTree clone = TypeUtil.cloneNoParent(this);
        Type root_type = clone.getRootType();
        // Create a new SchemaTree because the clone is ExtendedSchemaTree
        clone.setRootType(null);
        SchemaTree tree = new SchemaTree(root_type);
        
        // Copy over constraints
        for (Constraint c : new ArrayList<Constraint>(clone.getConstraints()))
        {
            clone.removeConstraint(c);
            tree.addConstraint(c);
        }
        
        return tree;
    }
    
    /**
     * Returns true if the type is an AnyScalarType or has a descendant that is an AnyScalarType.
     * 
     * @param type
     *            type
     * @return boolean
     */
    private boolean hasAnyScalarType(Type type)
    {
        if (type instanceof AnyScalarType)
        {
            return true;
        }
        else if (type instanceof ScalarType)
        {
            return false;
        }
        else if (type instanceof TupleType)
        {
            TupleType tuple_type = (TupleType) type;
            for (AttributeEntry entry : tuple_type)
            {
                Type child = entry.getType();
                if (hasAnyScalarType(child))
                {
                    return true;
                }
            }
            return false;
        }
        else if (type instanceof CollectionType)
        {
            CollectionType collection_type = (CollectionType) type;
            TupleType tuple_type = collection_type.getTupleType();
            return hasAnyScalarType(tuple_type);
        }
        else if (type instanceof SwitchType)
        {
            SwitchType switch_type = (SwitchType) type;
            for (String name : switch_type.getCaseNames())
            {
                Type child = switch_type.getCase(name);
                if (hasAnyScalarType(child))
                {
                    return true;
                }
            }
            return false;
        }
        
        // FIXME
        throw new UncheckedException("Unrecognized type: " + type);
        
    }
}
