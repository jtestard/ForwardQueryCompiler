/**
 * 
 */
package edu.ucsd.forward.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.ucsd.app2you.util.identity.IdentityHashSet;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.constraint.AutoIncrementConstraint;
import edu.ucsd.forward.data.constraint.Constraint;
import edu.ucsd.forward.data.constraint.LocalPrimaryKeyConstraint;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.NullType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.SwitchType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.TupleType.AttributeEntry;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeCloner;
import edu.ucsd.forward.data.type.TypeMap;
import edu.ucsd.forward.data.type.extended.AnyScalarType;
import edu.ucsd.forward.exception.UncheckedException;

/**
 * Utility methods to manipulate types.
 * 
 * @author Kian Win
 * @author Michalis Petropoulos
 * 
 */
public final class TypeUtil
{
    @SuppressWarnings("unused")
    private static final Logger log                 = Logger.getLogger(TypeUtil.class);
    
    private static Pattern      s_attr_name_pattern = Pattern.compile("[\\w&&[^0-9]][\\w.]*");
    
    /**
     * Private constructor.
     * 
     */
    private TypeUtil()
    {
    }
    
    /**
     * Determines if the first input type tree is isomorphic to the second input type tree, that is, there are tree structure
     * preserving mappings from the left to right, and vice versa.
     * 
     * @param <T>
     *            the type class.
     * @param left
     *            the left type.
     * @param right
     *            the right type.
     * @return <code>true</code> if two instances of type are equal by isomorphism; <code>false</code> otherwise.
     */
    public static <T extends Type> boolean deepEqualsByIsomorphism(T left, T right)
    {
        return deepEqualsByIsomorphism(left, right, true);
    }
    
    /**
     * Determines if the first input type tree is isomorphic to the second input type tree, that is, there are tree structure
     * preserving mappings from the left to right, and vice versa.
     * 
     * @param <T>
     *            the type class.
     * @param left
     *            the left type.
     * @param right
     *            the right type.
     * @param include_constraints
     *            whether to include constraints in the comparison.
     * @return <code>true</code> if two instances of type are equal by isomorphism; <code>false</code> otherwise.
     */
    public static <T extends Type> boolean deepEqualsByIsomorphism(T left, T right, boolean include_constraints)
    {
        if (deepEqualsByHomomorphism(left, right, include_constraints)
                && deepEqualsByHomomorphism(right, left, include_constraints)) return true;
        return false;
    }
    
    /**
     * Checks whether (a) left is homomorphic to right and (b) right is homomorphic to left except that right switch cases may be
     * unmapped.
     * 
     * @param <T>
     *            the type class.
     * @param left
     *            the left type.
     * @param right
     *            the right type.
     * @return <code>true</code> if two instances of type are equal by the above definition; <code>false</code> otherwise.
     */
    public static <T extends Type> boolean deepEqualsByIsomorphismWithRightSwitchCasesUnmapped(T left, T right)
    {
        if (deepEqualsByHomomorphism(left, right) && deepEqualsByHomomorphismExceptSwitchCases(right, left)) return true;
        return false;
    }
    
    /**
     * Determines if the input type tree is isomorphic to any of the type trees in the input collection.
     * 
     * @param collection
     *            a collection of type trees.
     * @param type
     *            a type tree.
     * @return <code>true</code> if two instances of type are equal by isomorphism; <code>false</code> otherwise.
     */
    public static boolean containByIsomorphism(Collection<? extends Type> collection, Type type)
    {
        for (Type type_i : collection)
        {
            if (deepEqualsByIsomorphism(type_i, type)) return true;
        }
        return false;
    }
    
    /**
     * Determines two type trees are equal by homomorphism in the sense that there is a tree structure preserving mapping from the
     * left type tree to the right one. If the type is schema tree, then also checks the equality of the associated constraints by
     * homomorphism.
     * 
     * @param <T>
     *            the type class.
     * @param left
     *            the left type
     * @param right
     *            the right type
     * @return <code>true</code> if two instances of type are equal by homomorphism; <code>false</code> otherwise.
     */
    public static <T extends Type> boolean deepEqualsByHomomorphism(T left, T right)
    {
        return deepEqualsByHomomorphism(left, right, true);
    }
    
    /**
     * Deeps union all the branches of the switch type.
     * 
     * @param switch_type
     *            the givens switch type.
     * @return the deep union result.
     */
    public static Type deepUnionSwitchType(SwitchType switch_type)
    {
        Type union_type = null;
        boolean inline_tuple = false;
        
        for (String case_name : switch_type.getCaseNames())
        {
            Type case_type = switch_type.getCase(case_name);
            if (union_type == null)
            {
                // first
                union_type = TypeUtil.cloneNoParent(case_type);
            }
            else
            {
                union_type = TypeUtil.deepUnion(union_type, case_type);
            }
            
            if (case_type instanceof TupleType)
            {
                inline_tuple = ((TupleType) case_type).isInline();
            }
        }
        
        if (inline_tuple) ((TupleType) union_type).setInline(true);
        
        return union_type;
    }
    
    /**
     * A test if two types can do deep union. FIXME: add the support of constraint.
     * 
     * @param <T>
     *            the type class.
     * @param left
     *            the left type
     * @param right
     *            the right type
     * @return <code>true</code> if two types can do deep union; <code>false</code> otherwise.
     */
    public static <T extends Type> boolean canDeepUnion(T left, T right)
    {
        if (left == right) return true;
        if (left == null) return false;
        if (right == null) return false;
        if (left instanceof NullType || right instanceof NullType) return true;
        if (left instanceof AnyScalarType && right instanceof ScalarType) return true;
        if (right instanceof AnyScalarType && left instanceof ScalarType) return true;
        if (left.getClass() != right.getClass()) return false;
        
        if (left instanceof CollectionType)
        {
            TupleType left_tuple = ((CollectionType) left).getTupleType();
            TupleType right_tuple = ((CollectionType) right).getTupleType();
            return canDeepUnion(left_tuple, right_tuple);
        }
        else if (left instanceof TupleType)
        {
            TupleType left_tuple = (TupleType) left;
            TupleType right_tuple = (TupleType) right;
            for (String attribute : right_tuple.getAttributeNames())
            {
                Type right_attribute_type = right_tuple.getAttribute(attribute);
                if (!left_tuple.hasAttribute(attribute))
                {
                    Type left_attribute_type = left_tuple.getAttribute(attribute);
                    if (!canDeepUnion(left_attribute_type, right_attribute_type)) return false;
                }
            }
            return true;
        }
        else if (left instanceof SwitchType)
        {
            throw new UnsupportedOperationException();
        }
        
        return true;
    }
    
    /**
     * A deep union of two types. FIXME: add the support of constraint.
     * 
     * @param <T>
     *            the type class.
     * @param left
     *            the left type
     * @param right
     *            the right type
     * @return deep union result
     */
    public static <T extends Type> T deepUnion(T left, T right)
    {
        assert left != null && right != null;
        if (left == right) return TypeUtil.cloneNoParent(left);
        
        if (left instanceof NullType)
        {
            return TypeUtil.cloneNoParent(right);
        }
        else if (right instanceof NullType)
        {
            return TypeUtil.cloneNoParent(left);
        }
        else if (left instanceof AnyScalarType || right instanceof AnyScalarType)
        {
            assert (left instanceof ScalarType && right instanceof ScalarType);
            return (T) (left.getClass() == right.getClass() ? TypeUtil.cloneNoParent(left) : new AnyScalarType());
        }
        else
        {
            assert left.getClass() == right.getClass();
            if (left instanceof TupleType)
            {
                TupleType tuple = new TupleType();
                TupleType left_tuple = (TupleType) left;
                TupleType right_tuple = (TupleType) right;
                
                // We do this to ensure the attributes appear in right order
                for (String attribute : left_tuple.getAttributeNames())
                {
                    Type left_attribute_type = left_tuple.getAttribute(attribute);
                    if (right_tuple.hasAttribute(attribute))
                    {
                        // deep union the attribute
                        tuple.setAttribute(attribute,
                                           deepUnion(left_attribute_type, right_tuple.getAttribute(attribute)));
                    }
                    else
                    {
                        tuple.setAttribute(attribute, TypeUtil.cloneNoParent(left_attribute_type));
                    }
                }
                for (String attribute : right_tuple.getAttributeNames())
                {
                    if (!left_tuple.hasAttribute(attribute))
                    {
                        tuple.setAttribute(attribute, TypeUtil.cloneNoParent(right_tuple.getAttribute(attribute)));
                    }
                }
                return (T) tuple;
            }
            else if (left instanceof CollectionType)
            {
                TupleType left_tuple = ((CollectionType) left).getTupleType();
                TupleType right_tuple = ((CollectionType) right).getTupleType();
                return (T) new CollectionType(deepUnion(left_tuple, right_tuple));
            }
            else if (left instanceof SwitchType)
            {
                throw new UnsupportedOperationException();
            }
        }
        return TypeUtil.cloneNoParent(left);
    }
    
    /**
     * Determines two type trees are equal by homomorphism in the sense that there is a tree structure preserving mapping from the
     * left type tree to the right one. If the type is schema tree, then also checks the equality of the associated constraints by
     * homomorphism.
     * 
     * @param <T>
     *            the type class.
     * @param left
     *            the left type
     * @param right
     *            the right type
     * @param include_constraints
     *            whether to include constraints in the comparison
     * @return <code>true</code> if two instances of type are equal by homomorphism; <code>false</code> otherwise.
     */
    public static <T extends Type> boolean deepEqualsByHomomorphism(T left, T right, boolean include_constraints)
    {
        if (left == right) return true;
        if (left == null) return false;
        if (right == null) return false;
        if (left instanceof NullType || right instanceof NullType) return true;
        if (left.getClass() != right.getClass()) return false;
        
        TypeMap type_map = getMapping(left, right);
        for (Type type : type_map.keySet())
        {
            if (type_map.get(type) == null) return false;
            // Compare constraints associated with the type
            // if (type instanceof CollectionType)
            // {
            // if (!deepEqualsByHomomorphism(((CollectionType) type).getConstraints(),
            // ((CollectionType) type_map.get(type)).getConstraints(),
            // type_map)) return false;
            // }
        }
        
        if (left instanceof SchemaTree && include_constraints)
        {
            // Check constrains equality
            List<Constraint> left_constraints = ((SchemaTree) left).getConstraints();
            List<Constraint> right_constraints = ((SchemaTree) right).getConstraints();
            if (!deepEqualsByHomomorphism(left_constraints, right_constraints, type_map)) return false;
        }
        
        return true;
    }
    
    /**
     * Determines two type trees are equal by homomorphism with the exception that switch cases in the left can be left unmapped.
     * 
     * @param <T>
     *            the type class.
     * @param left
     *            the left type
     * @param right
     *            the right type
     * @return <code>true</code> if two instances of type are equal by homomorphism with exception about switch cases;
     *         <code>false</code> otherwise.
     */
    public static <T extends Type> boolean deepEqualsByHomomorphismExceptSwitchCases(T left, T right)
    {
        
        if (left == right) return true;
        if (left == null) return false;
        if (right == null) return false;
        if (left.getClass() != right.getClass()) return false;
        
        TypeMap type_map = new TypeMap();
        addMapping(type_map, left, right, true);
        for (Type type : type_map.keySet())
        {
            if (type_map.get(type) == null) return false;
        }
        
        if (left instanceof SchemaTree)
        {
            // Check constrains equality
            List<Constraint> left_constraints = ((SchemaTree) left).getConstraints();
            List<Constraint> right_constraints = ((SchemaTree) right).getConstraints();
            
            for (Constraint left_constraint : left_constraints)
            {
                // Whether the checking can be skipped -- when the left constraint is in some unmapped switch case
                boolean skip = false;
                if (left_constraint instanceof LocalPrimaryKeyConstraint)
                {
                    CollectionType left_relation_type = ((LocalPrimaryKeyConstraint) left_constraint).getCollectionType();
                    // If the relation type is not mapped at all, then skip
                    if (type_map.get(left_relation_type) == null) skip = true;
                }
                else
                {
                    assert (left_constraint instanceof AutoIncrementConstraint);
                    ScalarType left_attribute = ((AutoIncrementConstraint) left).getAttribute();
                    // If the scalar type is not mapped at all, then skip
                    if (type_map.get(left_attribute) == null) skip = true;
                }
                
                // If this constraint cannot be skipped, then check
                if (!skip)
                {
                    boolean equal = false;
                    for (Constraint right_constraint : right_constraints)
                    {
                        if (deepEqualsByHomomorphism(left_constraint, right_constraint, type_map))
                        {
                            equal = true;
                            break;
                        }
                    }
                    if (!equal) return false;
                }
            }
        }
        
        return true;
    }
    
    /**
     * Determines two sets of constraints are equal by homomorphism.
     * 
     * @param left
     *            the left set of constraints
     * @param right
     *            the right set of constraints
     * @param type_map
     *            the type mapping of the type trees that the constraints associate with
     * @return @return <code>true</code> if the two sets of constraints are equal by homomorphism; <code>false</code> otherwise.
     */
    private static boolean deepEqualsByHomomorphism(List<Constraint> left, List<Constraint> right, TypeMap type_map)
    {
        for (Constraint left_constraint : left)
        {
            boolean equal = false;
            for (Constraint right_constraint : right)
            {
                if (deepEqualsByHomomorphism(left_constraint, right_constraint, type_map)) equal = true;
            }
            if (!equal) return false;
        }
        return true;
    }
    
    /**
     * Determines two constraints are equal by homomorphism.
     * 
     * @param left
     *            the left constraint
     * @param right
     *            the right constraint
     * @param type_map
     *            the type mapping of the type trees that the constraints associate with
     * @return <code>true</code> if two constraints are equal by homomorphism; <code>false</code> otherwise.
     */
    private static boolean deepEqualsByHomomorphism(Constraint left, Constraint right, TypeMap type_map)
    {
        if (left == right) return true;
        if (left.getClass() != right.getClass()) return false;
        
        if (left instanceof LocalPrimaryKeyConstraint)
        {
            CollectionType left_relation_type = ((LocalPrimaryKeyConstraint) left).getCollectionType();
            CollectionType right_relation_type = ((LocalPrimaryKeyConstraint) right).getCollectionType();
            if (type_map.get(left_relation_type) != right_relation_type) return false;
            List<ScalarType> left_attributes = ((LocalPrimaryKeyConstraint) left).getAttributes();
            List<ScalarType> right_attributes = ((LocalPrimaryKeyConstraint) right).getAttributes();
            for (ScalarType left_attribute : left_attributes)
            {
                ScalarType right_attribute = (ScalarType) type_map.get(left_attribute);
                if (right_attribute == null || !right_attributes.contains(right_attribute)) return false;
            }
        }
        else
        {
            assert (left instanceof AutoIncrementConstraint);
            
            ScalarType left_attribute = ((AutoIncrementConstraint) left).getAttribute();
            ScalarType right_attribute = ((AutoIncrementConstraint) right).getAttribute();
            if (type_map.get(left_attribute) != right_attribute) return false;
        }
        return true;
    }
    
    /**
     * Clones the schema tree, including its constraints, to which the input type belongs, and returns the clone of the input type
     * in the new schema tree.
     * 
     * Kevin: If you want to copy constraints as well, I have written one for LocalPrimaryKeyConstraint at DiffSchemaBuilder. We can
     * generalize this effort.
     * 
     * @param <T>
     *            - the type class.
     * @param type
     *            - the root node in the type tree.
     * @return the cloned type tree.
     */
    @SuppressWarnings("unchecked")
    public static <T extends Type> T clone(T type)
    {
        assert (type != null);
        
        Type old_root = type.getRoot();
        
        TypeCloner type_cloner = new TypeCloner();
        Type new_root = type_cloner.clone(old_root);
        
        if (new_root instanceof SchemaTree) return (T) new_root;
        
        Type new_type = new SchemaPath(type).find(new_root);
        
        return (T) new_type;
    }
    
    /**
     * Clones a type sub-tree. The type sub-tree will be deep-copied; its ancestors in the type tree will not. The constraints with
     * each copied type will be copied too.
     * 
     * @param <T>
     *            - the type class.
     * @param type
     *            - the type node.
     * @return the cloned type sub-tree.
     */
    public static <T extends Type> T cloneNoParent(T type)
    {
        assert (type != null);
        
        TypeCloner type_cloner = new TypeCloner();
        return type_cloner.clone(type);
    }
    
    /**
     * Returns a homomorphic mapping from the left type tree to the right type tree.
     * 
     * @param left
     *            - the left type tree.
     * @param right
     *            - the right type tree.
     * @return a homomorphic mapping from the left type tree to the right type tree.
     */
    public static TypeMap getMapping(Type left, Type right)
    {
        TypeMap map = new TypeMap();
        addMapping(map, left, right, false);
        return map;
    }
    
    /**
     * Builds a homomorphic mapping from the left type tree to the right type tree.
     * 
     * @param map
     *            - the homomorphic mapping.
     * @param left
     *            - the left type tree.
     * @param right
     *            - the right type tree.
     * @param switch_case_exception
     *            - whether to bypass a switch case that is not mapped
     */
    private static void addMapping(TypeMap map, Type left, Type right, boolean switch_case_exception)
    {
        Type right_type = null;
        
        // Always map from left to right
        map.put(left, right);
        
        if (left instanceof SchemaTree)
        {
            SchemaTree left_schema_tree = (SchemaTree) left;
            SchemaTree right_schema_tree = (SchemaTree) right;
            
            if (right != null) right_type = right_schema_tree.getRootType();
            
            Type left_root_type = left_schema_tree.getRootType();
            if (left_root_type.getClass() != right_type.getClass()) right_type = null;
            
            // Recurse
            addMapping(map, left_schema_tree.getRootType(), right_type, switch_case_exception);
        }
        else if (left instanceof TupleType)
        {
            if (right instanceof NullType) return;
            
            TupleType left_tuple_type = (TupleType) left;
            TupleType right_tuple_type = (TupleType) right;
            
            // Recurse for attribute names in common
            for (AttributeEntry entry : left_tuple_type)
            {
                Type left_value_type = entry.getType();
                right_type = null;
                if (right != null)
                {
                    Type right_value_type = right_tuple_type.getAttribute(entry.getName());
                    if (right_value_type != null)
                    {
                        if (left_value_type.getClass() == right_value_type.getClass())
                        {
                            right_type = right_value_type;
                        }
                        else if (left_value_type instanceof NullType || right_value_type instanceof NullType)
                        {
                            right_type = right_value_type;
                        }
                    }
                }
                addMapping(map, left_value_type, right_type, switch_case_exception);
            }
        }
        else if (left instanceof CollectionType)
        {
            CollectionType left_relation_type = (CollectionType) left;
            CollectionType right_relation_type = (CollectionType) right;
            
            if (right != null) right_type = right_relation_type.getTupleType();
            // Recurse
            addMapping(map, left_relation_type.getTupleType(), right_type, switch_case_exception);
        }
        else if (left instanceof SwitchType)
        {
            if (right instanceof NullType) return;
            
            SwitchType left_switch_type = (SwitchType) left;
            SwitchType right_switch_type = (SwitchType) right;
            
            // Recurse for case names in common
            for (String case_name : left_switch_type.getCaseNames())
            {
                right_type = null;
                Type left_value_type = left_switch_type.getCase(case_name);
                if (right != null && (right_switch_type.getCase(case_name)) != null)
                {
                    right_type = right_switch_type.getCase(case_name);
                }
                
                // When do we NOT map this tuple? Answer: When we allow switch_case_exception and the right type is null
                if (!(switch_case_exception && right_type == null))
                {
                    addMapping(map, left_value_type, right_type, switch_case_exception);
                }
            }
        }
        else
        {
            assert (left instanceof ScalarType || left instanceof NullType);
        }
    }
    
    /**
     * Attaches an existing <code>SchemaTree</code> as an attribute to an existing <code>TupleType</code>. This method automatically
     * copies all constraints from the old data set to the new one.
     * 
     * @param source
     *            The source <code>TupleType</code> in which the data set should be attached.
     * @param attribute_name
     *            The attribute name that it should be attached as.
     * @param to_attach
     *            The <code>SchemaTree</code> to attach.
     */
    @Deprecated
    public static void attachDataTree(TupleType source, String attribute_name, SchemaTree to_attach)
    {
        assert (source != null);
        assert (attribute_name != null);
        assert (to_attach != null);
        
        // Get the root type
        Type type_to_attach = to_attach.getRootType();
        
        // Detach it
        to_attach.setRootType(null);
        
        // Attach it
        source.setAttribute(attribute_name, type_to_attach);
        
        // Copy over constraints
        SchemaTree source_data_tree = (SchemaTree) source.getRoot();
        for (Constraint c : new ArrayList<Constraint>(to_attach.getConstraints()))
        {
            to_attach.removeConstraint(c);
            source_data_tree.addConstraint(c);
        }
        
    }
    
    /**
     * Detaches an existing <code>TupleType</code> as a new <code>SchemaTree</code>. This method automatically copies all
     * constraints into the new <code>SchemaTree</code>.
     * 
     * @param tuple_type
     *            The <code>TupleType</code> to detach.
     * @return A new <code>SchemaTree</code> containing the tuple.
     */
    public static SchemaTree detachAsSchemaTree(TupleType tuple_type)
    {
        assert (tuple_type != null);
        
        // Get the attribute name in the parent
        Type parent_type = tuple_type.getParent();
        assert (parent_type != null) : "Tuple must have a parent";
        assert (parent_type instanceof TupleType) : "Tuples parent is not a tuple";
        TupleType parent_tuple = (TupleType) parent_type;
        String attr_name = parent_tuple.getAttributeName(tuple_type);
        
        // Detach
        parent_tuple.removeAttribute(attr_name);
        
        // Create the new schema tree
        SchemaTree schema_tree = new SchemaTree(tuple_type);
        
        // Get the root type
        SchemaTree root_type = (SchemaTree) tuple_type.getRoot();
        
        // Move over constraints
        Collection<Type> descendants = tuple_type.getDescendants();
        for (Constraint constraint : new ArrayList<Constraint>(root_type.getConstraints()))
        {
            // Iterate over the types that the constraint applies to
            Set<Type> applied_types = new IdentityHashSet<Type>(constraint.getAppliedTypes());
            boolean applies = false;
            for (Type applied_type : applied_types)
            {
                if (descendants.contains(applied_type))
                {
                    applies = true;
                }
                else if (applies)
                {
                    // Remove the constrain because it only partially applies
                    root_type.removeConstraint(constraint);
                    applies = false;
                    break;
                }
                else
                {
                    break;
                }
            }
            
            // Move over the constraint if it applies
            if (applies)
            {
                root_type.removeConstraint(constraint);
                schema_tree.addConstraint(constraint);
            }
        }
        
        return schema_tree;
    }
    
    /**
     * Detaches a child type.
     * 
     * @param child_type
     *            the child type.
     */
    public static void detach(Type child_type)
    {
        
        Type parent_type = child_type.getParent();
        assert (parent_type != null);
        
        if (parent_type instanceof TupleType)
        {
            TupleType tuple_type = (TupleType) parent_type;
            String attribute_name = tuple_type.getAttributeName(child_type);
            tuple_type.removeAttribute(attribute_name);
        }
        else if (parent_type instanceof CollectionType)
        {
            CollectionType collection_type = (CollectionType) parent_type;
            collection_type.removeTupleType();
        }
        else if (parent_type instanceof SchemaTree)
        {
            SchemaTree schema_tree = (SchemaTree) parent_type;
            schema_tree.setRootType(null);
        }
        else
        {
            assert (parent_type instanceof SwitchType);
            SwitchType switch_type = (SwitchType) parent_type;
            switch_type.removeCase(child_type);
        }
    }
    
    /**
     * Checks if an attribute name is valid.
     * 
     * @param attr_name
     *            The name to check.
     * @return True if it is a valid attribute name, false if not.
     */
    public static boolean isValidAttributeName(String attr_name)
    {
        // return s_attr_name_pattern.matcher(attr_name).matches();
        // GWT compliant
        Matcher matcher = s_attr_name_pattern.matcher(attr_name);
        
        return (matcher.find() && matcher.start() == 0 && matcher.end() == attr_name.length());
    }
    
    /**
     * Checks whether the given type is constraint consistent.
     * 
     * @param type
     *            a given type.
     * @return <code>true</code> if the given type is constraint consistent; <code>false</code> otherwise.
     */
    public static boolean checkConstraintConsistent(Type type)
    {
        for (Constraint constraint : type.getConstraints())
        {
            if (!constraint.isTypeConsistent(type)) return false;
        }
        
        for (Type child : type.getChildren())
        {
            if (!checkConstraintConsistent(child)) return false;
        }
        
        return true;
    }
    
    /**
     * Checks that all collections in the tree rooted at the given type have primary key constraints.
     * 
     * @param type
     *            a given type.
     */
    public static void checkCollectionsHaveKeys(Type type)
    {
        Collection<? extends CollectionType> collections = type.getDescendantsAndSelf(CollectionType.class);
        for (CollectionType collection : collections)
        {
            if (collection.hasLocalPrimaryKeyConstraint())
            {
                continue;
            }
            
            // FIXME Use a proper data model exception
            throw new UncheckedException("Collection does not have an associated key : " + new SchemaPath(collection).getString());
        }
    }
    
}
