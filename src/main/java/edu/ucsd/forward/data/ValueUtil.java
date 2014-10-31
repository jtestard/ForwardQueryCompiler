/**
 * 
 */
package edu.ucsd.forward.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DataCloner;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.JsonValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.SwitchValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.TupleValue.AttributeValueEntry;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.data.value.XhtmlValue;
import edu.ucsd.forward.util.tree.Path;

/**
 * Utility methods to manipulate values.
 * 
 * @author Kian Win
 * @author Yupeng
 */
public final class ValueUtil
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ValueUtil.class);
    
    /**
     * Private constructor.
     * 
     */
    private ValueUtil()
    {
        
    }
    
    /**
     * Determines two instances of value are equal by value. Note that all collections are treated as sets.
     * 
     * @param left
     *            the left value
     * @param right
     *            the right value
     * @return <code>true</code> if two values are equal by value; <code>false</code> otherwise.
     */
    public static boolean deepEquals(Value left, Value right)
    {
        if (left.getClass() != right.getClass()) return false;
        
        if (left instanceof DataTree)
        {
            DataTree left_data_tree = (DataTree) left;
            DataTree right_data_tree = (DataTree) right;
            
            return deepEquals(left_data_tree.getRootValue(), right_data_tree.getRootValue());
        }
        else if (left instanceof CollectionValue)
        {
            CollectionValue left_collection = (CollectionValue) left;
            CollectionValue right_collection = (CollectionValue) right;
            
            if (left_collection.getTuples().size() != right_collection.getTuples().size()) return false;
            
            for (TupleValue left_tuple : left_collection.getTuples())
            {
                // Use set semantics to determine equality of two collections
                DataPath relative_path = new DataPath(left_collection, left_tuple);
                TupleValue right_tuple = (TupleValue) relative_path.find(right_collection);
                if (!deepEquals(left_tuple, right_tuple)) return false;
            }
            
            return true;
        }
        else if (left instanceof TupleValue)
        {
            TupleValue left_tuple = (TupleValue) left;
            TupleValue right_tuple = (TupleValue) right;
            
            if (!left_tuple.getAttributeNames().equals(right_tuple.getAttributeNames())) return false;
            
            for (AttributeValueEntry entry : left_tuple)
            {
                Value left_value = entry.getValue();
                Value right_value = right_tuple.getAttribute(entry.getName());
                if (!deepEquals(left_value, right_value)) return false;
            }
            return true;
        }
        else if (left instanceof SwitchValue)
        {
            SwitchValue left_switch = (SwitchValue) left;
            SwitchValue right_switch = (SwitchValue) right;
            
            if (!left_switch.getCaseName().equals(right_switch.getCaseName())) return false;
            
            return deepEquals(left_switch.getCase(), right_switch.getCase());
        }
        else if (left instanceof ScalarValue)
        {
            ScalarValue left_primitive = (ScalarValue) left;
            ScalarValue right_primitive = (ScalarValue) right;
            return left_primitive.deepEquals(right_primitive);
            
        }
        else if (left instanceof JsonValue)
        {
            JsonValue left_json = (JsonValue) left;
            JsonValue right_json = (JsonValue) right;
            return left_json.toString().equals(right_json.toString());
        }
        else
        {
            assert (left instanceof NullValue);
            return true;
        }
        
    }
    
    /**
     * Returns the value to which the specified key is mapped in this map, or null if the map contains no mapping for this key. The
     * key of the map is an instance of value in the data model, whose equality is defined by value, not java object.
     * 
     * @param <T>
     *            the class of the value in the map.
     * @param map
     *            the map whose key to be probed
     * @param key
     *            the key whose associated value is to be returned.
     * @return the value to which this map maps the specified key, or null if the map contains no mapping for this key.
     */
    public static <T> T getByValue(Map<? extends Value, T> map, Value key)
    {
        for (Value value : map.keySet())
        {
            if (deepEquals(value, key)) return map.get(value);
        }
        return null;
    }
    
    /**
     * Determines two lists of data are equal by value.
     * 
     * @param left
     *            the left list
     * @param right
     *            the right list
     * @return <code>true</code> if two lists of values are equal by value; <code>false</code> otherwise.
     */
    public static boolean deepEquals(List<? extends Value> left, List<? extends Value> right)
    {
        if (left.size() != right.size()) return false;
        for (int i = 0; i < left.size(); i++)
        {
            Value left_value = left.get(i);
            Value right_value = right.get(i);
            if (!deepEquals(left_value, right_value)) return false;
        }
        return true;
    }
    
    /**
     * Determines two sets of values are equal by value.
     * 
     * @param left
     *            the left set
     * @param right
     *            the right set
     * @return <code>true</code> if two sets of values are equal by value; <code>false</code> otherwise.
     */
    public static boolean deepEquals(Set<? extends Value> left, Set<? extends Value> right)
    {
        for (Value left_value : left)
        {
            boolean equal = false;
            for (Value right_value : right)
            {
                if (deepEquals(left_value, right_value)) equal = true;
            }
            if (!equal) return false;
        }
        for (Value right_value : right)
        {
            boolean equal = false;
            for (Value left_value : left)
            {
                if (deepEquals(left_value, right_value)) equal = true;
            }
            if (!equal) return false;
        }
        return true;
    }
    
    /**
     * Clones a data tree. The data tree, type tree and constraints will be deep-copied.
     * 
     * @param <T>
     *            the value class.
     * @param value
     *            the root value in the data tree.
     * @return the cloned data tree.
     */
    public static <T extends Value> T clone(T value)
    {
        assert value != null : "The value to be cloned should not be null.";
        assert value.getParent() == null : "The value to be cloned should not have parent.";
        
        DataCloner data_cloner = new DataCloner();
        data_cloner.setTypeCloning(true);
        
        T value_copy = data_cloner.clone(value);
        if (value_copy instanceof DataTree)
        {
            DataTree data_tree = ((DataTree) value_copy);
            if (((DataTree) value).isConsistent()) data_tree.checkConsistent();
        }
        return value_copy;
    }
    
    /**
     * Clones a data tree. Only the data tree will be deep-copied; the associated type tree and constraints will not.
     * 
     * @param <T>
     *            the value class.
     * @param value
     *            the value.
     * @return the cloned data tree.
     */
    public static <T extends Value> T cloneNoType(T value)
    {
        assert value != null;
        assert value.getParent() == null;
        
        DataCloner data_cloner = new DataCloner();
        data_cloner.setTypeCloning(false);
        return data_cloner.clone(value);
    }
    
    /**
     * Clones a data sub-tree. Only the value and its descendants will be deep-copied; its ancestors in the data tree, and the
     * associated type tree and constraints will not.
     * 
     * @param <T>
     *            the value class.
     * @param value
     *            the value.
     * @return the cloned data sub-tree.
     */
    public static <T extends Value> T cloneNoParentNoType(T value)
    {
        assert (value != null);
        
        DataCloner data_cloner = new DataCloner();
        data_cloner.setTypeCloning(false);
        return data_cloner.clone(value);
    }
    
    /**
     * Given a data tree and a value in the data tree, remove any tuple that is not under the value and not an ancestor of the
     * value.
     * 
     * @param data_tree
     *            the data tree
     * @param value
     *            the value
     */
    public static void removeAllOtherTuples(DataTree data_tree, Value value)
    {
        assert (value.getDataTree() == data_tree);
        
        Value current_value = value;
        Value parent_value = value.getParent();
        
        while (parent_value != null)
        {
            if (parent_value instanceof CollectionValue)
            {
                assert (current_value instanceof TupleValue);
                CollectionValue relation = (CollectionValue) parent_value;
                TupleValue tuple = (TupleValue) current_value;
                
                ArrayList<TupleValue> list = new ArrayList<TupleValue>(relation.getTuples());
                
                for (TupleValue child : list)
                {
                    if (child != tuple) relation.remove(child);
                }
            }
            
            current_value = parent_value;
            parent_value = current_value.getParent();
        }
    }
    
    /**
     * Returns the value instance that a hierarchical path navigates to.
     * 
     * @param value
     *            the start of the path.
     * @param path
     *            the hierarchical path.
     * @return the value instance navigated to.
     */
    @Deprecated
    public static Value get(Value value, String path)
    {
        assert value != null;
        assert path != null;
        
        return get(value, new Path(path));
    }
    
    /**
     * Returns true if the path to navigate to in the value is valid, false otherwise.
     * 
     * @param value
     *            value to traverse
     * @param path
     *            path to navigate
     * @return whether the path is valid
     */
    @Deprecated
    public static boolean isValidPath(Value value, Path path)
    {
        assert value != null;
        assert path != null;
        
        // Base case: all parts have been matched
        if (path.isEmpty()) return true;
        
        // Divide the parts into the first part and other parts
        String first_part = path.get();
        Path sub_path = path.subPath();
        
        if (value instanceof DataTree)
        {
            // Navigating from a data tree into its root value is automatic
            Value root_value = ((DataTree) value).getRootValue();
            return isValidPath(root_value, path);
        }
        else if (value instanceof TupleValue)
        {
            // Navigating from a tuple into its attribute is via the attribute name
            TupleValue tuple = (TupleValue) value;
            Value child = tuple.getAttribute(first_part);
            if (child == null) return false;
            return isValidPath(child, sub_path);
        }
        else if (value instanceof CollectionValue)
        {
            // Navigating from a relation into its tuple is via the 0-based index
            int index = -1;
            try
            {
                index = Integer.parseInt(first_part);
            }
            catch (NumberFormatException e)
            {
                assert false : "\"" + index + "\" is not a valid integer";
            }
            CollectionValue relation = (CollectionValue) value;
            assert index >= 0 : index + " is out of range";
            if (index >= relation.getTuples().size()) return false;
            TupleValue tuple = ((CollectionValue) value).getTuples().get(index);
            return isValidPath(tuple, sub_path);
        }
        else if (value instanceof SwitchValue)
        {
            // Navigating from a switch value into a case tuple requires matching the case name
            SwitchValue switch_value = (SwitchValue) value;
            Value case_value = switch_value.getCase();
            if (!switch_value.getCaseName().equals(first_part)) return false;
            return isValidPath(case_value, sub_path);
        }
        else
        {
            assert value instanceof NullValue;
            
            return false;
        }
    }
    
    /**
     * Returns true if the path to navigate to in the value is valid, false otherwise.
     * 
     * @param value
     *            value to traverse
     * @param path
     *            path to navigate
     * @return whether the path is valid
     */
    @Deprecated
    public static boolean isValidPath(Value value, String path)
    {
        assert value != null;
        assert path != null;
        
        return isValidPath(value, new Path(path));
    }
    
    /**
     * Returns true if the path to navigate to in the value is valid, setting output_result[0] to the retrieved value, false
     * otherwise.
     * 
     * @param value
     *            value to traverse
     * @param path
     *            path to navigate
     * @param output_result
     *            If this array is not null, output_result[0] will be set to the retrieved value.
     * @return whether the path is valid
     */
    @Deprecated
    public static boolean isValidPath(Value value, String path, Value[] output_result)
    {
        if (isValidPath(value, path))
        {
            output_result[0] = get(value, path);
            return true;
        }
        return false;
    }
    
    /**
     * Returns the value instance that a hierarchical path navigates to.
     * 
     * @param value
     *            the start of the path.
     * @param path
     *            the hierarchical path.
     * @return the value instance navigated to.
     */
    @Deprecated
    public static Value get(Value value, Path path)
    {
        assert value != null;
        assert path != null;
        
        // Base case: all parts have been matched
        if (path.isEmpty()) return value;
        
        // Divide the parts into the first part and other parts
        String first_part = path.get();
        Path sub_path = path.subPath();
        
        if (value instanceof DataTree)
        {
            // Navigating from a data tree into its root value is automatic
            Value root_value = ((DataTree) value).getRootValue();
            return get(root_value, path);
        }
        else if (value instanceof TupleValue)
        {
            // Navigating from a tuple into its attribute is via the attribute name
            TupleValue tuple = (TupleValue) value;
            Value child = tuple.getAttribute(first_part);
            assert child != null : "Attribute \"" + first_part + "\" cannot be found";
            return get(child, sub_path);
        }
        else if (value instanceof CollectionValue)
        {
            // Navigating from a relation into its tuple is via the 0-based index
            int index = -1;
            try
            {
                index = Integer.parseInt(first_part);
            }
            catch (NumberFormatException e)
            {
                assert false : "\"" + index + "\" is not a valid integer";
            }
            CollectionValue relation = (CollectionValue) value;
            assert index >= 0 : index + " is out of range";
            assert index < relation.getTuples().size() : index + " is out of range - relation has " + relation.getTuples().size()
                    + " tuples";
            TupleValue tuple = ((CollectionValue) value).getTuples().get(index);
            return get(tuple, sub_path);
        }
        else
        {
            assert value instanceof SwitchValue;
            
            // Navigating from a switch value into a case tuple requires matching the case name
            SwitchValue switch_value = (SwitchValue) value;
            Value case_value = switch_value.getCase();
            assert switch_value.getCaseName().equals(first_part) : first_part + " is invalid - switch value has case name \""
                    + switch_value.getCaseName() + "\"";
            return get(case_value, sub_path);
        }
    }
    
    /**
     * Returns a homomorphic mapping from the left data tree to the right data tree. If some value in the left does not have mapping
     * to the right, put a node-to-null pair in the mapping.
     * 
     * @param left
     *            the left data tree.
     * @param right
     *            the right data tree.
     * @return a homomorphic mapping from the left data tree to the right data tree.
     */
    public static ValueMap getMapping(Value left, Value right)
    {
        assert (left != null);
        ValueMap map = new ValueMap();
        addMapping(map, left, right);
        return map;
    }
    
    /**
     * Builds a homomorphic mapping from the left data tree to the right data tree.
     * 
     * @param map
     *            the homomorphic mapping.
     * @param left
     *            the left data tree.
     * @param right
     *            the right data tree.
     */
    private static void addMapping(ValueMap map, Value left, Value right)
    {
        Value right_value = null;
        if (left instanceof DataTree)
        {
            DataTree left_data_tree = (DataTree) left;
            DataTree right_data_tree = (DataTree) right;
            
            // Always map from left to right
            map.put(left, right);
            
            if (right != null) right_value = right_data_tree.getRootValue();
            // Recurse
            addMapping(map, left_data_tree.getRootValue(), right_value);
        }
        else if (left instanceof TupleValue)
        {
            TupleValue left_tuple = (TupleValue) left;
            TupleValue right_tuple = (TupleValue) right;
            
            // Always map from left to right
            map.put(left, right);
            
            // Recurse for attribute names in common
            for (AttributeValueEntry entry : left_tuple)
            {
                Value left_value = entry.getValue();
                right_value = null;
                if (right_tuple != null)
                {
                    right_value = right_tuple.getAttribute(entry.getName());
                }
                addMapping(map, left_value, right_value);
            }
        }
        else if (left instanceof CollectionValue)
        {
            CollectionValue left_relation = (CollectionValue) left;
            CollectionValue right_relation = (CollectionValue) right;
            
            // Always map from left to right
            map.put(left, right);
            
            // Recurse for tuples in common
            for (int i = 0; i < left_relation.getTuples().size(); i++)
            {
                TupleValue left_tuple = left_relation.getTuples().get(i);
                
                DataPath path = new DataPath(left_relation, left_tuple);
                
                right_value = path.find(right_relation);
                assert right_value instanceof TupleValue;
                addMapping(map, left_tuple, right_value);
            }
        }
        else if (left instanceof SwitchValue)
        {
            SwitchValue left_switch = (SwitchValue) left;
            SwitchValue right_switch = (SwitchValue) right;
            
            map.put(left, right);
            
            if (right_switch != null && left_switch.getCaseName().equals(right_switch.getCaseName()))
            {
                right_value = right_switch.getCase();
            }
            
            // Recurse for case tuple
            addMapping(map, left_switch.getCase(), right_value);
        }
        else if (left instanceof NullValue)
        {
            // Always map from left to right
            map.put(left, right);
        }
        else
        {
            assert (left instanceof ScalarValue);
            
            ScalarValue left_primitive = (ScalarValue) left;
            ScalarValue right_primitive = (ScalarValue) right;
            
            if (right != null)
            {
                if (left_primitive instanceof XhtmlValue && right_primitive instanceof XhtmlValue)
                {
                    XhtmlValue left_xhtml = (XhtmlValue) left_primitive;
                    XhtmlValue right_xhtml = (XhtmlValue) right_primitive;
                    if (left_xhtml.toString().equals(right_xhtml.toString())) right_value = right;
                }
                else
                {
                    // Map to left to right only if the values are equal
                    if (left_primitive.getObject().equals(right_primitive.getObject())) right_value = right;
                }
            }
            map.put(left, right_value);
        }
    }
    
    /**
     * Sets the associated type to <code>null</code> for the given value and all its descendants.
     * 
     * @param value
     *            the value.
     */
    public static void unsetTypeRecursively(Value value)
    {
        for (Value child : value.getChildren())
        {
            unsetTypeRecursively(child);
        }
        
        value.setType(null);
    }
    
    /**
     * Attaches an existing <code>DataTree</code> as an attribute to an existing <code>Tuple</code>.
     * 
     * @param source
     *            The source <code>Tuple</code> in which the data tree should be attached.
     * @param attribute_name
     *            The attribute name that it should be attached as.
     * @param to_attach
     *            The <code>DataTree</code> to attach.
     */
    public static void attachDataTree(TupleValue source, String attribute_name, DataTree to_attach)
    {
        assert (source != null);
        assert (attribute_name != null);
        assert (to_attach != null);
        
        // Get the value
        Value value_to_attach = to_attach.getRootValue();
        
        // Detach it
        to_attach.setRootValue(new TupleValue());
        
        // Attach it
        source.setAttribute(attribute_name, value_to_attach);
    }
    
    /**
     * Detaches an existing <code>Tuple</code> as a new <code>DataTree</code>.
     * 
     * @param tuple
     *            The <code>Tuple</code> to detach.
     * @return A new <code>DataTree</code> containing the tuple.
     */
    public static DataTree detachAsDataTree(TupleValue tuple)
    {
        assert (tuple != null);
        
        // Get the attribute name in the parent
        Value parent = tuple.getParent();
        assert (parent != null) : "Tuple must have a parent";
        assert (parent instanceof TupleValue) : "Tuples parent is not a tuple";
        TupleValue parent_tuple = (TupleValue) parent;
        String attr_name = parent_tuple.getAttributeName(tuple);
        
        // Detach
        parent_tuple.removeAttribute(attr_name);
        
        // Create the new data tree
        DataTree data_tree = new DataTree(tuple);
        
        return data_tree;
    }
    
    /**
     * Replaces the old value with the new value. Note that after the replacement the data tree is modified, so the caller ought to
     * check the consistency of the data tree after this operation.
     * 
     * @param old_value
     *            the old value
     * @param new_value
     *            the new value
     */
    public static void replace(Value old_value, Value new_value)
    {
        assert old_value != null;
        assert new_value != null;
        // TODO Yupeng: Probably we may want to check the types of the new value and old value are compatible
        // Romain: Yes, it is necessary. Here's a first try:
        // Erick : Shouldn't we be calling ValueTypeMapUtil.map instead? Otherwise the descendants will have inconsistent type
        //      if so remove ValueTypeMapUtil.map call in MappingUtil.updateValue
        if (old_value.getType() != null)
        {
            new_value.setType(old_value.getType());
        }
        
        Value parent = old_value.getParent();
        assert parent != null;
        // Get the name of the value
        String name;
        if (parent instanceof TupleValue)
        {
            TupleValue tuple = (TupleValue) parent;
            name = tuple.getAttributeName(old_value);
        }
        else if (parent instanceof SwitchValue)
        {
            SwitchValue switch_value = (SwitchValue) parent;
            name = switch_value.getCaseName();
            // Directly replace the switch tuple
        }
        else if (parent instanceof CollectionValue)
        {
            name = "tuple";
        }
        else
        {
            assert parent instanceof DataTree;
            name = "/";
        }
        // No need to detach the old value.
        
        // Attach new value
        attach(parent, name, new_value);
    }
    
    /**
     * Attaches a child value to a parent value. If the parent expects only one value, the new value will replace the old one.
     * 
     * @param parent_value
     *            the parent value.
     * @param name
     *            the name to use for an attribute or a case.
     * @param child_value
     *            the child value.
     */
    public static void attach(Value parent_value, String name, Value child_value)
    {
        // TODO: The name parameter is unnecessary if a detached value holds onto its edge label
        
        if (parent_value instanceof TupleValue)
        {
            TupleValue tuple_value = (TupleValue) parent_value;
            tuple_value.setAttribute(name, child_value);
        }
        else if (parent_value instanceof CollectionValue)
        {
            CollectionValue collection_value = (CollectionValue) parent_value;
            collection_value.add((TupleValue) child_value);
        }
        else if (parent_value instanceof DataTree)
        {
            DataTree data_tree = (DataTree) parent_value;
            data_tree.setRootValue(child_value);
        }
        else
        {
            assert (parent_value instanceof SwitchValue);
            
            SwitchValue switch_value = (SwitchValue) parent_value;
            switch_value.setCase(name, child_value);
        }
    }
    
    /**
     * Detaches a child value.
     * 
     * @param child_value
     *            the child value.
     */
    public static void detach(Value child_value)
    {
        
        Value parent_value = child_value.getParent();
        if (parent_value == null) return;
        
        if (parent_value instanceof TupleValue)
        {
            TupleValue tuple_value = (TupleValue) parent_value;
            String attribute_name = tuple_value.getAttributeName(child_value);
            tuple_value.removeAttribute(attribute_name);
        }
        else if (parent_value instanceof CollectionValue)
        {
            CollectionValue collection_value = (CollectionValue) parent_value;
            collection_value.remove((TupleValue) child_value);
        }
        else if (parent_value instanceof DataTree)
        {
            DataTree data_tree = (DataTree) parent_value;
            data_tree.setRootValue(null);
        }
        else
        {
            assert (parent_value instanceof SwitchValue);
            // FIXME:Yupeng: Do we allow detaching the switch value's selected tuple?
            SwitchValue switch_value = (SwitchValue) parent_value;
            String case_name = switch_value.getCaseName();
            switch_value.setCase(case_name, null);
        }
    }
    
    /**
     * Gets the closet collection tuple of the given value. Return <code>null</code> if not exists.
     * 
     * @param value
     *            the specific value
     * @return the closet collection tuple of the given value. Return <code>null</code> if not exists.
     */
    public static TupleValue getClosetCollectionTuple(Value value)
    {
        TupleValue tuple = null;
        List<Value> ancestors = value.getAncestorsAndSelf();
        Collections.reverse(ancestors);
        for (Value ancestor : ancestors)
        {
            if (ancestor instanceof TupleValue && ancestor.getParent() instanceof CollectionValue)
            {
                assert ancestor instanceof TupleValue;
                tuple = (TupleValue) ancestor;
                break;
            }
        }
        return tuple;
    }
    
    /**
     * Gets the attribute specified as a string, returning null if the attribute is null or not found.
     * 
     * @param tuple
     *            tuple
     * @param attribute
     *            attribute
     * @return attribute value or null
     */
    public static String getString(TupleValue tuple, String attribute)
    {
        Value value = tuple.getAttribute(attribute);
        if (value != null && value instanceof StringValue)
        {
            return ((StringValue) value).toString();
        }
        assert value == null || value instanceof NullValue;
        return null;
    }
    
    /**
     * Gets the attribute specified as a integer, returning null if the attribute is null or not found.
     * 
     * @param tuple
     *            tuple
     * @param attribute
     *            attribute
     * @return attribute value, null if not found
     */
    public static Integer getInteger(TupleValue tuple, String attribute)
    {
        Value value = tuple.getAttribute(attribute);
        if (value != null && value instanceof IntegerValue)
        {
            return ((IntegerValue) value).getObject();
        }
        assert value == null || value instanceof NullValue;
        return null;
    }
    
    /**
     * Gets the attribute specified as a boolean, returning null if the attribute is null or not found.
     * 
     * @param tuple
     *            tuple
     * @param attribute
     *            attribute
     * @return attribute value, null if not found
     */
    public static Boolean getBoolean(TupleValue tuple, String attribute)
    {
        Value value = tuple.getAttribute(attribute);
        if (value != null && value instanceof BooleanValue)
        {
            return ((BooleanValue) value).getObject();
        }
        assert value == null || value instanceof NullValue;
        return null;
    }
}
