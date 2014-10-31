/**
 * 
 */
package edu.ucsd.forward.data.type;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.constraint.AutoIncrementConstraint;
import edu.ucsd.forward.data.constraint.Constraint;
import edu.ucsd.forward.data.constraint.ForeignKeyConstraint;
import edu.ucsd.forward.data.constraint.GlobalUniquenessConstraint;
import edu.ucsd.forward.data.constraint.HomogeneousConstraint;
import edu.ucsd.forward.data.constraint.LocalPrimaryKeyConstraint;
import edu.ucsd.forward.data.constraint.LocalUniquenessConstraint;
import edu.ucsd.forward.data.constraint.NonNullConstraint;
import edu.ucsd.forward.data.constraint.SingletonConstraint;
import edu.ucsd.forward.data.index.IndexDeclaration;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.type.TupleType.AttributeEntry;
import edu.ucsd.forward.data.type.extended.AnyScalarType;

/**
 * A cloner for type trees.
 * 
 * @author Kian Win
 * 
 */
public class TypeCloner
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TypeCloner.class);
    
    /**
     * Construct a type cloner.
     */
    public TypeCloner()
    {
        
    }
    
    /**
     * Clones a type sub-tree. The type sub-tree will be deep-copied; its ancestors in the type tree and the associated data tree
     * will not.
     * 
     * @param <T>
     *            - the type class.
     * @param type
     *            - the type node.
     * @return the cloned type sub-tree.
     */
    public <T extends Type> T clone(T type)
    {
        return cloneManual(type);
    }
    
    /**
     * Clones all the constraints from the old typye to the new type.
     * 
     * @param new_type
     *            the new type
     * @param old_type
     *            the old type
     */
    protected void cloneConstraints(Type new_type, Type old_type)
    {
        for (Constraint old_constraint : old_type.getConstraints())
        {
            cloneConstraint(old_constraint, new_type, old_type);
            // FIXME: No need to add the cloned constraint to the cloned type since this is happening in the constraint constructor.
            // But this is inconsistent with different constraints.
        }
    }
    
    /**
     * Clones one given constraint which is from the old type.
     * 
     * @param constraint
     *            the constraint to clone
     * @param new_type
     *            the new type that the constraint is clone to.
     * @param old_type
     *            the old type that the constraint is from.
     * @return the cloned constraint
     */
    private Constraint cloneConstraint(Constraint constraint, Type new_type, Type old_type)
    {
        Constraint new_constraint = null;
        if (constraint instanceof AutoIncrementConstraint)
        {
            new_constraint = new AutoIncrementConstraint((ScalarType) new_type);
        }
        else if (constraint instanceof ForeignKeyConstraint)
        {
            throw new UnsupportedOperationException();
        }
        else if (constraint instanceof GlobalUniquenessConstraint)
        {
            throw new UnsupportedOperationException();
        }
        else if (constraint instanceof HomogeneousConstraint)
        {
            throw new UnsupportedOperationException();
        }
        else if (constraint instanceof LocalPrimaryKeyConstraint)
        {
            List<Type> attributes_in_new = new ArrayList<Type>();
            for (SchemaPath schema_path : ((LocalPrimaryKeyConstraint) constraint).getAttributesSchemaPaths())
            {
                ScalarType attribute_in_new_tree = (ScalarType) schema_path.find(new_type);
                attributes_in_new.add(attribute_in_new_tree);
            }
            new_constraint = new LocalPrimaryKeyConstraint((CollectionType) new_type, attributes_in_new);
        }
        else if (constraint instanceof LocalUniquenessConstraint)
        {
            throw new UnsupportedOperationException();
        }
        else if (constraint instanceof NonNullConstraint)
        {
            new_constraint = new NonNullConstraint(new_type);
        }
        else
        {
            assert constraint instanceof SingletonConstraint;
            new_constraint = new SingletonConstraint((CollectionType) new_type);
        }
        return new_constraint;
    }
    
    /**
     * Clones a type sub-tree by manually traversing the type tree and calling respective constructors. The type sub-tree will be
     * deep-copied; the associated data tree will not.
     * 
     * @param <T>
     *            - the type class.
     * @param type
     *            - the type node.
     * @return the cloned type sub-tree.
     */
    @SuppressWarnings("unchecked")
    protected <T extends Type> T cloneManual(T type)
    {
        Type type_copy = null;
        if (type instanceof SchemaTree)
        {
            SchemaTree old_schema_tree = (SchemaTree) type;
            Type new_root_type = cloneManual(old_schema_tree.getRootType());
            SchemaTree new_tree = new SchemaTree(new_root_type);
            type_copy = new_tree;
        }
        else if (type instanceof TupleType)
        {
            TupleType old_tuple_type = (TupleType) type;
            TupleType new_tuple_type = new TupleType();
            new_tuple_type.setInline(old_tuple_type.isInline());
            for (AttributeEntry entry : old_tuple_type)
            {
                Type new_value_type = cloneManual(entry.getType());
                new_tuple_type.setAttribute(entry.getName(), new_value_type);
            }
            type_copy = new_tuple_type;
        }
        else if (type instanceof CollectionType)
        {
            CollectionType old_coll_type = (CollectionType) type;
            TupleType new_tuple_type = cloneManual(old_coll_type.getTupleType());
            CollectionType new_coll_type = new CollectionType(new_tuple_type);
            type_copy = new_coll_type;
            // Clone indices
            for (IndexDeclaration declaration : old_coll_type.getIndexDeclarations())
            {
                List<ScalarType> keys = new ArrayList<ScalarType>();
                for (SchemaPath path : declaration.getKeyPaths())
                {
                    ScalarType key = (ScalarType) path.find(old_coll_type);
                    keys.add(key);
                }
                IndexDeclaration new_dec = new IndexDeclaration(declaration.getName(), new_coll_type, keys,
                                                                declaration.isUnique(), declaration.getMethod());
                try
                {
                    new_coll_type.addIndex(new_dec);
                }
                catch (DataSourceException e)
                {
                    assert false;
                }
            }
        }
        else if (type instanceof SwitchType)
        {
            SwitchType old_switch_type = (SwitchType) type;
            SwitchType new_switch_type = new SwitchType();
            new_switch_type.setInline(old_switch_type.isInline());
            for (String case_name : old_switch_type.getCaseNames())
            {
                Type new_tuple_type = cloneManual(old_switch_type.getCase(case_name));
                new_switch_type.setCase(case_name, new_tuple_type);
            }
            type_copy = new_switch_type;
        }
        else if (type instanceof StringType)
        {
            type_copy = new StringType();
        }
        else if (type instanceof IntegerType)
        {
            type_copy = new IntegerType();
        }
        else if (type instanceof DoubleType)
        {
            type_copy = new DoubleType();
        }
        else if (type instanceof FloatType)
        {
            type_copy = new FloatType();
        }
        else if (type instanceof LongType)
        {
            type_copy = new LongType();
        }
        else if (type instanceof DecimalType)
        {
            type_copy = new DecimalType();
        }
        else if (type instanceof TimestampType)
        {
            type_copy = new TimestampType();
        }
        else if (type instanceof DateType)
        {
            type_copy = new DateType();
        }
        else if (type instanceof BooleanType)
        {
            type_copy = new BooleanType();
        }
        else if (type instanceof XhtmlType)
        {
            type_copy = new XhtmlType();
        }
        else if (type instanceof NullType)
        {
            type_copy = new NullType();
        }
        else if (type instanceof AnyScalarType)
        {
            type_copy = new AnyScalarType();
        }
        else if (type instanceof JsonType)
        {
            type_copy = new JsonType();
        }
        else if (type instanceof UnknownType)
        {
            type_copy = new UnknownType();
        }
        else
        {
            throw new AssertionError();
        }
        
        type_copy.setDefaultValue(type.getDefaultValue());
        
        cloneConstraints(type_copy, type);
        return (T) type_copy;
    }
}
