/**
 * 
 */
package edu.ucsd.forward.data;

import static edu.ucsd.forward.data.DataPathStep.ATTRIBUTE_EQUAL;
import static edu.ucsd.forward.data.DataPathStep.ATTRIBUTE_NAME_PATTERN;
import static edu.ucsd.forward.data.DataPathStep.ATTRIBUTE_VALUE_PATTERN;
import static edu.ucsd.forward.data.DataPathStep.PREDICATE_END;
import static edu.ucsd.forward.data.DataPathStep.PREDICATE_SEPARATOR;
import static edu.ucsd.forward.data.DataPathStep.PREDICATE_START;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.constraint.PrimaryKeyUtil;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.SwitchValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.util.tree.AbstractTreePath;

/**
 * A data path.
 * 
 * COMMENT: Kevin: This data path does not work with singleton collection.
 * 
 * @author Kian Win Ong
 * @author Michalis Petropoulos
 * 
 */
public class DataPath extends AbstractTreePath<DataPath, DataPathStep>
{
    @SuppressWarnings("unused")
    private static final Logger  log                     = Logger.getLogger(DataPath.class);
    
    public static final DataPath ABSOLUTE_EMPTY;
    
    public static final DataPath RELATIVE_EMPTY;
    
    static
    {
        try
        {
            ABSOLUTE_EMPTY = new DataPath("/");
            RELATIVE_EMPTY = new DataPath("");
        }
        catch (ParseException e)
        {
            throw new AssertionError(e);
        }
    }
    
    /*
     * A data path has string representation:
     * 
     * name / name [ key1 = value1, key2 = value2 ... ] / name ...
     * 
     * The data path steps are separated by slashes. Within each step, the name is compulsory, whereas the predicate (enclosed in
     * square brackets) is optional.
     * 
     * Within the predicate, the key attribute names can contain only alphanumeric characters, underscore and period. The values are
     * URL-encoded, which guarantees that the only special characters that can occur are .-*_+%
     */

    public static final String   PATH_SEPARATOR          = "/";
    
    public static final String   ATTRIBUTE_PATTERN       = ATTRIBUTE_NAME_PATTERN + Pattern.quote(ATTRIBUTE_EQUAL)
                                                                 + ATTRIBUTE_VALUE_PATTERN;
    
    public static final String   ATTRIBUTES_LIST_PATTERN = ATTRIBUTE_PATTERN + "(" + Pattern.quote(PREDICATE_SEPARATOR)
                                                                 + ATTRIBUTE_PATTERN + ")*";
    
    // public static final String PREDICATE_PATTERN = Pattern.quote(PREDICATE_START) + "(" + ATTRIBUTES_LIST_PATTERN + ")"
    // + Pattern.quote(PREDICATE_END);
    //    
    // public static final Pattern PATH_STEP_PATTERN = Pattern.compile("(" + ATTRIBUTE_NAME_PATTERN + ")" + "("
    // + PREDICATE_PATTERN + ")?");
    // GWT compliant
    public static final Pattern  PREDICATE_PATTERN       = Pattern.compile("(" + ATTRIBUTES_LIST_PATTERN + ")");
    
    /**
     * Constructs a data path.
     * 
     * @param path_string
     *            the path in string format.
     * @throws ParseException
     *             if an error occurs during parsing.
     */
    public DataPath(String path_string) throws ParseException
    {
        super(parse(path_string), path_string.startsWith(PATH_SEPARATOR) ? PathMode.ABSOLUTE : PathMode.RELATIVE);
    }
    
    /**
     * Constructs a data path.
     * 
     * @param value
     *            the value to refer to.
     */
    public DataPath(Value value)
    {
        super(createPathSteps(value), PathMode.ABSOLUTE);
    }
    
    /**
     * Constructs a relative data path from source value to target value.
     * 
     * @param source_value
     *            the source value.
     * @param target_value
     *            the target value.
     */
    public DataPath(Value source_value, Value target_value)
    {
        super(createPathSteps(source_value, target_value), PathMode.RELATIVE);
    }
    
    /**
     * Constructs a data path.
     * 
     * @param path_steps
     *            the path steps.
     * @param path_mode
     *            the path mode.
     */
    public DataPath(List<DataPathStep> path_steps, PathMode path_mode)
    {
        super(path_steps, path_mode);
    }
    
    /**
     * Constructs a data path from a type node and its global primary keys in string.
     * 
     * @param type
     *            the type node
     * @param global_primary_keys
     *            the global primary keys (before any URL encode), in the order generated by
     *            PrimaryKeyUtil.getGlobalPrimaryKeyValues().
     */
    public DataPath(Type type, List<String> global_primary_keys)
    {
        super(createPathStepsFromTypeAndGlobalPrimaryKeys(type, global_primary_keys), PathMode.ABSOLUTE);
    }
    
    /**
     * Finds the matching value in a data tree.
     * 
     * @param value
     *            the value to start matching from.
     * @return the matching value if it exists; <code>null</code> otherwise.
     */
    public Value find(Value value)
    {
        assert (value != null);
        
        LinkedList<DataPathStep> steps = new LinkedList<DataPathStep>(getPathSteps());
        
        if (value instanceof DataTree)
        {
            return find(((DataTree) value).getRootValue(), steps);
        }
        else if (getPathMode().equals(PathMode.ABSOLUTE))
        {
            // Get the root value based on whether the value is attached to a data tree
            DataTree data_tree = value.getDataTree();
            Value root_value = (data_tree != null) ? data_tree.getRootValue() : value.getRoot();
            
            // Match an absolute path starting from the root value
            return find(root_value, steps);
        }
        else
        {
            // Match a relative path starting from the given value
            return find(value, steps);
        }
    }
    
    /**
     * Converts a data path to its corresponding schema path.
     * 
     * @return the corresponding schema path.
     */
    public SchemaPath toSchemaPath()
    {
        // Retain the names, but omit the predicates
        List<String> schema_path_steps = new ArrayList<String>();
        for (DataPathStep data_path_step : getPathSteps())
        {
            schema_path_steps.add(data_path_step.getName());
        }
        return new SchemaPath(schema_path_steps, getPathMode());
    }
    
    @Override
    protected DataPath newPath(List<DataPathStep> path_steps, PathMode path_mode)
    {
        return new DataPath(path_steps, path_mode);
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
    private static List<DataPathStep> parse(String path_string) throws ParseException
    {
        assert (path_string != null);
        
        // Omit the leading separator, if any
        String omit_leading_separator = path_string.startsWith(PATH_SEPARATOR) ? path_string.substring(1) : path_string;
        
        if (omit_leading_separator.endsWith(PATH_SEPARATOR))
        {
            String s = "Invalid data path " + path_string + " has trailing slash";
            throw new ParseException(s, path_string.length() - 1);
        }
        
        // Nothing to parse
        if (omit_leading_separator.isEmpty()) return Collections.emptyList();
        
        // Split the string into respective path steps
        List<String> path_step_strings = Arrays.asList(omit_leading_separator.split(PATH_SEPARATOR));
        
        List<DataPathStep> path_steps = new ArrayList<DataPathStep>();
        for (String path_step_string : path_step_strings)
        {
            // Parse a path step using a regex
            // Matcher matcher = PATH_STEP_PATTERN.matcher(path_step_string);
            // if (!matcher.matches())
            // GWT compliant
            Matcher attr_matcher = ATTRIBUTE_NAME_PATTERN.matcher(path_step_string);
            if (!attr_matcher.find() && attr_matcher.start() == 0)
            {
                String s = "Invalid step " + path_step_string + " of data path " + path_string;
                throw new ParseException(s, 0);
            }
            
            // Get the name
            // String name_string = matcher.group(1);
            // GWT compliant
            String name_string = path_step_string.substring(attr_matcher.start(), attr_matcher.end());
            
            // Get the predicate (if any)
            // String predicate_string = matcher.group(3);
            // GWT compliant
            String predicate_string = path_step_string.substring(attr_matcher.end());
            if (!predicate_string.isEmpty())
            {
                predicate_string = predicate_string.substring(PREDICATE_START.length(), predicate_string.length()
                        - PREDICATE_END.length());
                Matcher pred_matcher = PREDICATE_PATTERN.matcher(predicate_string);
                if (!pred_matcher.find() || pred_matcher.start() != 0 || pred_matcher.end() != predicate_string.length())
                {
                    String s = "Invalid step " + path_step_string + " of data path " + path_string;
                    throw new ParseException(s, 0);
                }
                
                // Split the predicate into attribute name / value pairs
                Map<String, String> predicate = new LinkedHashMap<String, String>();
                String[] attributes = predicate_string.split(PREDICATE_SEPARATOR);
                for (String attribute : attributes)
                {
                    String[] pair = attribute.split(ATTRIBUTE_EQUAL);
                    String attribute_name = pair[0];
                    String attribute_value = pair[1];
                    predicate.put(attribute_name, attribute_value);
                }
                path_steps.add(new DataPathStep(name_string, predicate));
            }
            else
            {
                path_steps.add(new DataPathStep(name_string));
            }
        }
        return path_steps;
    }
    
    /**
     * Creates the path steps that corresponds to a value.
     * 
     * @param value
     *            the value.
     * @return the path steps.
     */
    private static List<DataPathStep> createPathSteps(Value value)
    {
        assert (value != null);
        
        DataTree data_tree = value.getDataTree();
        assert (data_tree != null) : "A data path can only be created for a value "
                + " that is attached to a data tree and primary key constraints";
        
        // Iterate in root-to-leaf order
        List<DataPathStep> path_steps = new ArrayList<DataPathStep>();
        for (Value v : value.getValueAncestorsAndSelf())
        {
            // Skip invalid path names
            DataPathStep path_step = createPathStep(v);
            if (path_step == null) continue;
            
            path_steps.add(path_step);
        }
        
        return path_steps;
    }
    
    /**
     * Creates a list of DataPathSteps from a given type node and its global primary key values.
     * 
     * @param type
     *            the type node
     * @param global_primary_keys
     *            the global primary key values
     * @return a list of DataPathSteps
     */
    private static List<DataPathStep> createPathStepsFromTypeAndGlobalPrimaryKeys(Type type, List<String> global_primary_keys)
    {
        assert (type != null);
        assert (global_primary_keys != null);
        
        /*
         * TODO: The current implementation is slightly inefficient, as getPathName() needs to access sibling attributes of types in
         * the root-to-leaf path. This will be fixed when AbstractTreeNode stores the incoming edge's label on a node.
         */

        List<DataPathStep> path_steps = new ArrayList<DataPathStep>();
        
        Iterator<String> key_itr = global_primary_keys.iterator();
        
        // Iterate in root-to-leaf order
        for (Type t : type.getTypeAncestorsAndSelf())
        {
            String path_name = SchemaPath.getPathName(t);
            
            // Skip the root type, which does not have a path name
            if (path_name == null) continue;
            
            if (t instanceof TupleType && t.getParent() instanceof CollectionType)
            {
                // A collection tuple needs to be identified with a predicate comprising its local primary key values
                Map<String, String> map = new LinkedHashMap<String, String>();
                CollectionType collection = (CollectionType) t.getParent();
                List<ScalarType> local_keys = collection.getLocalPrimaryKeyConstraint().getAttributes();
                for (ScalarType local_key : local_keys)
                {
                    // TODO: Change some assertions to exceptions
                    assert (key_itr.hasNext()) : "Not enough primary key values to construct a data path";
                    String attribute_name = SchemaPath.getPathName(local_key);
                    assert (attribute_name != null) : "Cannot find path step name for a local primary key";
                    map.put(attribute_name, DataPathStep.encode(key_itr.next()));
                }
                
                assert (map.size() == local_keys.size()) : "Local primary key attributes must have unique names";
                path_steps.add(new DataPathStep(path_name, map));
            }
            else
            {
                path_steps.add(new DataPathStep(path_name));
            }
        }
        return path_steps;
    }
    
    /**
     * Creates the path steps for a relative path from source value to target value.
     * 
     * @param source_value
     *            the source value.
     * @param target_value
     *            the target value.
     * @return the path steps.
     */
    private static List<DataPathStep> createPathSteps(Value source_value, Value target_value)
    {
        assert (source_value != null);
        assert (target_value != null);
        
        DataPath source_path = new DataPath(source_value);
        DataPath target_path = new DataPath(target_value);
        assert (source_path.isPrefix(target_path)) : (source_path + " is not an ancestor of " + target_path);
        return target_path.relative(source_path.getLength()).getPathSteps();
    }
    
    /**
     * Creates a path step that corresponds to a value without considering its ancestors. A path step is not applicable for the root
     * value.
     * 
     * @param value
     *            the value.
     * @return the path step, if applicable; <code>null</code> otherwise.
     */
    public static DataPathStep createPathStep(Value value)
    {
        /*
         * TODO: The current implementation is slightly inefficient, as getPathName() needs to access sibling attributes of values
         * in the root-to-leaf path. This will be fixed when AbstractTreeNode stores the incoming edge's label on a node.
         */

        String path_name = getPathName(value);
        
        // Skip the root value, which does not have a path name
        if (path_name == null) return null;
        
        if (value instanceof TupleValue && value.getParent() instanceof CollectionValue)
        {
            // A collection tuple needs to be identified with a predicate comprising its local primary key values
            
            // Get the local primary key values
            List<ScalarValue> primary_key_values = PrimaryKeyUtil.getLocalPrimaryKeyValues((TupleValue) value);
            Map<String, String> map = new LinkedHashMap<String, String>();
            for (ScalarValue primary_key_value : primary_key_values)
            {
                // Get the attribute name
                String attribute_name = getPathName(primary_key_value);
                
                // URL-encode the attribute value
                map.put(attribute_name, DataPathStep.encode(primary_key_value));
            }
            assert (map.size() == primary_key_values.size()) : "Local primary key attributes must have unique names";
            return new DataPathStep(path_name, map);
        }
        else
        {
            return new DataPathStep(path_name);
        }
    }
    
    /**
     * Returns the name that refers to the value within a path. The name is inapplicable for the root value.
     * 
     * @param value
     *            the value.
     * @return the name that refers to the value within a path, if applicable; <code>null</code> otherwise.
     */
    private static String getPathName(Value value)
    {
        Value parent = value.getParent();
        
        // Inapplicable for root value (detached from data tree)
        if (parent == null) return null;
        
        // Inapplicable for data tree
        if (parent instanceof DataTree) return null;
        
        if (parent instanceof TupleValue)
        {
            TupleValue parent_tuple_value = (TupleValue) parent;
            return parent_tuple_value.getAttributeName(value);
        }
        else if (parent instanceof CollectionValue)
        {
            return "tuple";
        }
        else
        {
            assert (parent instanceof SwitchValue);
            SwitchValue parent_switch_value = (SwitchValue) parent;
            return parent_switch_value.getCaseName();
        }
    }
    
    /**
     * Finds the matching value, starting from the given value and using the given path steps.
     * 
     * @param value
     *            the value to start matching from.
     * @param path_steps
     *            the path steps.
     * @return the matching value if it exists; <code>null</code> otherwise.
     */
    private static Value find(Value value, Queue<DataPathStep> path_steps)
    {
        DataPathStep path_step = path_steps.poll();
        
        // All path steps have been matched, return the value
        if (path_step == null) return value;
        
        if (value instanceof TupleValue)
        {
            if (path_step.getPredicate() != null)
            {
                // Too expensive to use during production
                // log.debug("Cannot match attribute as path step {} has a predicate", path_step);
                return null;
            }
            
            // Match the child value with the same name
            TupleValue tuple_value = (TupleValue) value;
            Value child_value = tuple_value.getAttribute(path_step.getName());
            if (child_value != null)
            {
                return find(child_value, path_steps);
            }
            else
            {
                // Too expensive to use during production
                // log.debug("Cannot match attribute with path step {}", path_step);
                return null;
            }
        }
        else if (value instanceof CollectionValue)
        {
            if (!path_step.getName().equals("tuple"))
            {
                // Too expensive to use during production
                // log.debug("Cannot match collection tuple as path step {} does not have name \"tuple\"", path_step);
                return null;
            }
            if (path_step.getPredicate() == null)
            {
                // Too expensive to use during production
                // log.debug("Cannot match collection tuple as path step {} does not have a predicate", path_step);
                return null;
            }
            
            // Match the tuple value with the same primary key value
            CollectionValue collection_value = (CollectionValue) value;
            for (TupleValue tuple_value : collection_value.getTuples())
            {
                /*
                 * For ease of implementation, we re-use createPathStep() here to create the corresponding path step for each tuple.
                 * Unfortunately, this requires the given data tree to always have a schema.
                 * 
                 * TODO: Relax this requirement by navigating the data tree to find the correct tuple, i.e. one with key values
                 * equal to those in the given predicate.
                 */
                DataPathStep tuple_path_step = createPathStep(tuple_value);
                if (path_step.equals(tuple_path_step))
                {
                    return find(tuple_value, path_steps);
                }
            }
            // Too expensive to use during production
            // log.debug("Cannot match collection tuple with path step {}", path_step);
            return null;
        }
        else if (value instanceof SwitchValue)
        {
            if (path_step.getPredicate() != null)
            {
                // Too expensive to use during production
                // log.debug("Cannot match switch case as path step {} has a predicate", path_step);
                return null;
            }
            
            // Check that the selected case has the same name
            SwitchValue switch_value = (SwitchValue) value;
            String case_name = switch_value.getCaseName();
            if (!case_name.equals(path_step.getName()))
            {
                // Too expensive to use during production
                // log.debug("Cannot match selected case name {} with path step {}", case_name, path_step);
                return null;
            }
            
            return find(switch_value.getCase(), path_steps);
        }
        else
        {
            // The match cannot succeed, as there are remaining path steps, but a scalar value or a null value is a leaf node
            assert (value instanceof ScalarValue || value instanceof NullValue);
            // Too expensive to use during production
            // log.debug("Cannot match any value with path step {}", path_step);
            return null;
        }
    }
    
}
