/**
 * 
 */
package edu.ucsd.forward.data;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.*;
import edu.ucsd.forward.data.value.*;
import edu.ucsd.forward.util.tree.AbstractTreePath;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A schema path.
 * 
 * @author Kian Win Ong
 * 
 */
@SuppressWarnings("serial")
public class SchemaPath extends AbstractTreePath<SchemaPath, String>
{
    private static final Logger    log             = Logger.getLogger(SchemaPath.class);
    
    public static final String     PATH_SEPARATOR  = "/";
    
    public static final SchemaPath ABSOLUTE_EMPTY;
    
    public static final SchemaPath RELATIVE_EMPTY;
    
    public static final String     TUPLE_PATH_STEP = "tuple";
    
    static
    {
        try
        {
            ABSOLUTE_EMPTY = new SchemaPath("/");
            RELATIVE_EMPTY = new SchemaPath("");
        }
        catch (ParseException e)
        {
            throw new AssertionError(e);
        }
    }
    
    /**
     * Constructs a schema path.
     * 
     * @param path_string
     *            the path in string format.
     * @throws ParseException
     *             if an error occurs during parsing.
     */
    public SchemaPath(String path_string) throws ParseException
    {
        super(parse(path_string), path_string.startsWith(PATH_SEPARATOR) ? PathMode.ABSOLUTE : PathMode.RELATIVE);
    }
    
    /**
     * Constructs an absolute schema path.
     * 
     * @param type
     *            the type to refer to.
     */
    public SchemaPath(Type type)
    {
        super(createPathSteps(type), PathMode.ABSOLUTE);
    }
    
    /**
     * Constructs an absolute schema path of a value. The value does not have to be mapped with a type.
     * 
     * @param value
     *            the value to refer to.
     */
    public SchemaPath(Value value)
    {
        super(createPathSteps(value), PathMode.ABSOLUTE);
    }
    
    /**
     * Default constructor.
     */
    protected SchemaPath()
    {
        
    }
    
    /**
     * Constructs a relative schema path from source type to target type.
     * 
     * @param source_type
     *            the source type.
     * @param target_type
     *            the target type.
     */
    public SchemaPath(Type source_type, Type target_type)
    {
        super(createPathSteps(source_type, target_type), PathMode.RELATIVE);
    }
    
    /**
     * Constructs a schema path.
     * 
     * @param path_steps
     *            the path steps.
     * @param path_mode
     *            the path mode.
     */
    public SchemaPath(List<String> path_steps, PathMode path_mode)
    {
        super(path_steps, path_mode);
    }
    
    /**
     * Constructs a schema path.
     * 
     * @param path_mode
     *            the path mode.
     * @param path_steps
     *            the path steps.
     */
    public SchemaPath(PathMode path_mode, String... path_steps)
    {
        super(Arrays.asList(path_steps), path_mode);
    }
    
    /**
     * Constructs a schema path navigating to the root.
     * 
     * @return a schema path navigating to the root.
     */
    public static SchemaPath root()
    {
        return new SchemaPath(new ArrayList<String>(), PathMode.ABSOLUTE);
    }
    
    /**
     * Finds the matching type in a type tree.
     * 
     * @param type
     *            the type to start matching from.
     * @return the matching type if it exists; <code>null</code> otherwise.
     */
    public Type find(Type type)
    {
        assert (type != null);
        
        if (type instanceof SchemaTree)
        {
            return find(((SchemaTree) type).getRootType(), getPathSteps());
        }
        else if (getPathMode().equals(PathMode.ABSOLUTE))
        {
            // Get the root type based on whether the type is attached to a schema tree
            SchemaTree schema_tree = type.getSchemaTree();
            Type root_type = (schema_tree != null) ? schema_tree.getRootType() : (Type) type.getRoot();
            
            // Match an absolute path starting from the root type
            return find(root_type, getPathSteps());
        }
        else
        {
            // Match a relative path starting from the given type
            return find(type, getPathSteps());
        }
    }
    
    /**
     * Finds the matching list of values in a data tree in the order they were encountered in a depth-first left-to-right traversal
     * of the tree.
     * 
     * @param value
     *            the value to start matching from.
     * @return the matching list of values.
     */
    public List<Value> find(Value value)
    {
        assert (value != null);
        
        Value starting_value = (value instanceof DataTree) ? ((DataTree) value).getRootValue() : value;
        
        if (value instanceof DataTree)
        {
            return find(((DataTree) value).getRootValue(), getPathSteps());
        }
        else if (getPathMode().equals(PathMode.ABSOLUTE))
        {
            // Get the root value based on whether the value is attached to a data tree
            DataTree data_tree = starting_value.getDataTree();
            Value root_value = (data_tree != null) ? data_tree.getRootValue() : starting_value.getRoot();
            
            // Match an absolute path starting from the root value
            return find(root_value, getPathSteps());
        }
        else
        {
            // Match a relative path starting from the given value
            return find(starting_value, getPathSteps());
        }
    }
    
    @Override
    protected SchemaPath newPath(List<String> path_steps, PathMode path_mode)
    {
        return new SchemaPath(path_steps, path_mode);
    }
    
    @Override
    public String getSeparator()
    {
        return PATH_SEPARATOR;
    }
    
    /**
     * Parses a string representation of the path into respective path steps.
     * 
     * @param path_string
     *            the path in string format.
     * @return the path steps.
     * @throws ParseException
     *             if an error occurs during parsing.
     */
    private static List<String> parse(String path_string) throws ParseException
    {
        assert (path_string != null);
        
        // Omit the leading separator, if any
        String omit_leading_separator = path_string.startsWith(PATH_SEPARATOR) ? path_string.substring(1) : path_string;
        
        if (omit_leading_separator.endsWith(PATH_SEPARATOR))
        {
            String s = "Invalid schema path " + path_string + " has trailing slash";
            throw new ParseException(s, path_string.length() - 1);
        }
        
        // Nothing to parse
        if (omit_leading_separator.isEmpty()) return Collections.emptyList();
        
        // Split the string into respective path steps
        return Arrays.asList(omit_leading_separator.split(PATH_SEPARATOR));
    }
    
    /**
     * Creates the path steps that corresponds to a type.
     * 
     * @param type
     *            the type.
     * @return the path steps.
     */
    private static List<String> createPathSteps(Type type)
    {
        assert (type != null);
        
        /*
         * TODO: The current implementation is slightly inefficient, as getPathName() needs to access sibling attributes of types in
         * the root-to-leaf path. This will be fixed when AbstractTreeNode stores the incoming edge's label on a node.
         */
        
        List<String> path_steps = new ArrayList<String>();
        
        // Iterate in root-to-leaf order
        for (Type t : type.getTypeAncestorsAndSelf())
        {
            String path_step = getPathName(t);
            
            // Skip the root type, which does not have a path name
            if (path_step == null) continue;
            
            path_steps.add(path_step);
        }
        return path_steps;
    }
    
    /**
     * Creates the path steps that corresponds to the type of a value, without actually having the type around.
     * 
     * @param value
     *            the value.
     * @return the path steps.
     */
    private static List<String> createPathSteps(Value value)
    {
        assert (value != null);
        
        /*
         * TODO: The current implementation is slightly inefficient, as getPathName() needs to access sibling attributes of types in
         * the root-to-leaf path. This will be fixed when AbstractTreeNode stores the incoming edge's label on a node.
         */
        
        List<String> path_steps = new ArrayList<String>();
        
        // Iterate in root-to-leaf order
        for (Value v : value.getValueAncestorsAndSelf())
        {
            String path_step = getPathName(v);
            
            // Skip the root type, which does not have a path name
            if (path_step == null) continue;
            
            path_steps.add(path_step);
        }
        return path_steps;
    }
    
    /**
     * Creates the path steps for a relative path from source type to target type.
     * 
     * @param source_type
     *            the source type.
     * @param target_type
     *            the target type.
     * @return the path steps.
     */
    private static List<String> createPathSteps(Type source_type, Type target_type)
    {
        assert (source_type != null);
        assert (target_type != null);
        
        SchemaPath source_path = new SchemaPath(source_type);
        SchemaPath target_path = new SchemaPath(target_type);
        assert (source_path.isPrefix(target_path)) : (source_path + " is not an ancestor of " + target_path);
        return target_path.relative(source_path.getLength()).getPathSteps();
    }
    
    /**
     * Returns the name that refers to the type within a path. The name is inapplicable for the root type.
     * 
     * @param type
     *            the type.
     * @return the name that refers to the type within a path, if applicable; <code>null</code> otherwise.
     */
    public static String getPathName(Type type)
    {
        Type parent = type.getParent();
        
        // Inapplicable for root type (detached from schema tree)
        if (parent == null) return null;
        
        // Inapplicable for schema tree
        if (parent instanceof SchemaTree) return null;
        
        if (parent instanceof CollectionType)
        {
            return TUPLE_PATH_STEP;
        }
        else if (parent instanceof SwitchType)
        {
            SwitchType parent_switch_type = (SwitchType) parent;
            return parent_switch_type.getCaseName(type);
        }
        else
        {
            assert (parent instanceof TupleType);
            TupleType parent_tuple_type = (TupleType) parent;
            return parent_tuple_type.getAttributeName(type);
        }
        
    }
    
    /**
     * Returns the name that refers to the type within a path. The name is inapplicable for the root type.
     * 
     * @param value
     *            the type.
     * @return the name that refers to the type within a path, if applicable; <code>null</code> otherwise.
     */
    public static String getPathName(Value value)
    {
        Value parent = value.getParent();
        
        // Inapplicable for root type (detached from schema tree)
        if (parent == null) return null;
        
        // Inapplicable for schema tree
        if (parent instanceof DataTree) return null;
        
        if (parent instanceof CollectionValue)
        {
            return TUPLE_PATH_STEP;
        }
        else if (parent instanceof SwitchValue)
        {
            SwitchValue parent_switch_value = (SwitchValue) parent;
            return parent_switch_value.getCaseName();
        }
        else
        {
            assert (parent instanceof TupleValue);
            TupleValue parent_tuple_value = (TupleValue) parent;
            return parent_tuple_value.getAttributeName(value);
        }
        
    }
    
    /**
     * Finds the matching type, starting from the given type and using the given path steps.
     * 
     * TODO: Kevin: this is tail-recursion. Consider changing it to non-recursive version to boost speed.
     * 
     * @param type
     *            the type to start matching from.
     * @param path_steps
     *            the path steps.
     * @return the matching type if it exists; <code>null</code> otherwise.
     */
    public static Type find(Type type, List<String> path_steps)
    {
        assert (type != null);
        assert (path_steps != null);
        
        // All path steps have been matched, return the type
        if (path_steps.size() == 0) return type;
        
        // Otherwise, split the path steps into (1) the next path step to match and (2) the remaining path steps
        String path_step = path_steps.get(0);
        List<String> remaining = path_steps.subList(1, path_steps.size());
        
        if (type instanceof TupleType)
        {
            TupleType tuple_type = (TupleType) type;
            Type child_type = tuple_type.getAttribute(path_step);
            if (child_type != null)
            {
                return find(child_type, remaining);
            }
            else
            {
                log.debug("Cannot match attribute with path step {}", path_step);
                return null;
            }
        }
        else if (type instanceof CollectionType)
        {
            CollectionType collection_type = (CollectionType) type;
            TupleType child_tuple_type = collection_type.getTupleType();
            if (path_step.equals(TUPLE_PATH_STEP))
            {
                return find(child_tuple_type, remaining);
            }
            else
            {
                log.debug("Cannot match collection tuple type with path step {}", path_step);
                return null;
            }
        }
        else if (type instanceof SwitchType)
        {
            SwitchType switch_type = (SwitchType) type;
            Type case_type = switch_type.getCase(path_step);
            if (case_type != null)
            {
                return find(case_type, remaining);
            }
            else
            {
                log.debug("Cannot match case with path step {}", path_step);
                return null;
            }
        }
        else
        {
            // The match cannot succeed, as there are remaining path steps, but a scalar type is a leaf node
            assert (type instanceof ScalarType);
            log.debug("Cannot match any type with path step {}", path_step);
            return null;
        }
    }
    
    /**
     * Finds the matching list of values, starting from the given value and using the given path steps, and returns them in the
     * order they were encountered in a depth-first left-to-right traversal of the tree.
     * 
     * @param value
     *            the value to start matching from.
     * @param path_steps
     *            the path steps.
     * @return the matching list of values.
     */
    private static List<Value> find(Value value, List<String> path_steps)
    {
        // All path steps have been matched, return the value
        if (path_steps.size() == 0) return Collections.singletonList(value);
        
        // Otherwise, split the path steps into (1) the next path step to match and (2) the remaining path steps
        String path_step = path_steps.get(0);
        List<String> remaining = path_steps.subList(1, path_steps.size());
        
        if (value instanceof TupleValue)
        {
            // Match the child value with the same name
            TupleValue tuple_value = (TupleValue) value;
            Value child_value = tuple_value.getAttribute(path_step);
            if (child_value != null)
            {
                return find(child_value, remaining);
            }
            else
            {
                // FIXME Throw an invalid schema path or inconsistent data tree exception.
                throw new AssertionError();
            }
        }
        else if (value instanceof CollectionValue)
        {
            List<Value> result = new ArrayList<Value>();
            
            if (!path_step.equals(TUPLE_PATH_STEP))
            {
                // FIXME Throw an invalid schema path or inconsistent data tree exception.
                throw new AssertionError();
            }
            
            // Match all tuple values since there is no predicate
            CollectionValue collection_value = (CollectionValue) value;
            for (TupleValue tuple_value : collection_value.getTuples())
            {
                result.addAll(find(tuple_value, remaining));
            }
            
            return result;
        }
        else if (value instanceof SwitchValue)
        {
            // Check that the selected case has the same name
            SwitchValue switch_value = (SwitchValue) value;
            String case_name = switch_value.getCaseName();
            if (!case_name.equals(path_step))
            {
                return Collections.emptyList();
            }
            
            return find(switch_value.getCase(), remaining);
        }
        else if (value instanceof ScalarValue)
        {
            // The match cannot succeed, as there are remaining path steps, but a scalar value is a leaf node
            // FIXME Throw an invalid schema path or inconsistent data tree exception.
            throw new AssertionError();
        }
        else
        {
            assert (value instanceof NullValue);
            
            return Collections.emptyList();
        }
    }
    
    /**
     * Converts a schema path to its corresponding data path.
     * 
     * @return the corresponding data path.
     */
    public DataPath toDataPath()
    {
        List<DataPathStep> path_steps = new ArrayList<DataPathStep>();
        for (String path_step : getPathSteps())
        {
            DataPathStep data_path_step = new DataPathStep(path_step);
            path_steps.add(data_path_step);
        }
        return new DataPath(path_steps, getPathMode());
    }
    
    /**
     * Returns a shared instance of relative schema path of zero steps.
     * 
     * @return the schema path.
     */
    public static SchemaPath getZeroStepRelativeSchemaPath()
    {
        return new SchemaPath(Collections.<String> emptyList(), PathMode.RELATIVE);
    }
}
