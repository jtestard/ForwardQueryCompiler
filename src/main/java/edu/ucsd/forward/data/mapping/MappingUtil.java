/**
 * 
 */
package edu.ucsd.forward.data.mapping;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.DataPath;
import edu.ucsd.forward.data.ValueTypeMapUtil;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessorFactory;

/**
 * A utility class that manipulates the mapped data.
 * 
 * @author Yupeng Fu
 * 
 */
public final class MappingUtil
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(MappingUtil.class);
    
    /**
     * Hidden constructor.
     */
    private MappingUtil()
    {
        
    }
    
    /**
     * Updates the mapped value to the new value.
     * 
     * @param src_value
     *            the source value in the mapping.
     * @param new_value
     *            the new value.
     * @throws DataSourceException
     * @throws QueryExecutionException
     */
    public static void updateValue(Value src_value, Value new_value) throws DataSourceException, QueryExecutionException
    {
        Value to_value = findTargetValueInMappedObject(src_value);
        if (to_value == null) return;
        
        Value new_value_copy = ValueUtil.cloneNoParentNoType(new_value);
        // We have to map the types to maintain consistency
        if (src_value.getType() != null)
        {
            ValueTypeMapUtil.map(new_value_copy, src_value.getType());
        }
        ValueUtil.replace(to_value, new_value_copy);
    }
    
    /**
     * Adds the tuple to the mapped collection.
     * 
     * @param src_collection
     *            the source collection in the mapping.
     * @param tuple
     *            the tuple to add.
     * @throws DataSourceException
     * @throws QueryExecutionException
     */
    public static void addTuple(CollectionValue src_collection, TupleValue tuple)
            throws DataSourceException, QueryExecutionException
    {
        Value to_value = findTargetValueInMappedObject(src_collection);
        if (to_value == null) return;
        
        Value cloned_value = ValueUtil.cloneNoParentNoType(tuple);
        // We have to map the types to maintain consistency
        if (tuple.getType() != null)
        {
            ValueTypeMapUtil.map(cloned_value, tuple.getType());
        }
        
        CollectionValue to_collection = (CollectionValue) to_value;
        to_collection.add(cloned_value);
    }
    
    private static Value findTargetValueInMappedObject(Value src_value) throws DataSourceException, QueryExecutionException
    {
        List<MappingGroup> groups = QueryProcessorFactory.getInstance().getUnifiedApplicationState().getMappingGroups();
        if (groups.isEmpty()) return null;
        
        MappingGroup group = groups.get(0);
        UnifiedApplicationState uas = QueryProcessorFactory.getInstance().getUnifiedApplicationState();
        DataSource mapping_from_source = uas.getDataSource(group.getFromSource());
        DataTree edit_from_object = src_value.getDataTree();
        DataSource edit_from_source = edit_from_object.getDataSource();
        if (mapping_from_source != edit_from_source) return null;
        
        if (!mapping_from_source.hasSchemaObject(group.getFromObject())) return null;
        DataTree mapping_from_object = mapping_from_source.getDataObject(group.getFromObject());
        if (mapping_from_object != edit_from_object) return null;
        
        DataSource to_source = uas.getDataSource(group.getToSource());
        DataTree to_object = to_source.getDataObject(group.getToObject());
        
        DataPath src_data_path = new DataPath(src_value);
        String root_attr_path_str = DataPath.PATH_SEPARATOR + src_data_path.getPathSteps().get(0).getName();
        Mapping mapping = group.getMapping(root_attr_path_str);
        
        // Get target attribute
        DataPath target_path = mapping.getTargetPath();
        assert target_path != null;
        Value to_attr = target_path.find(to_object);
        Value from_root_attr = mapping.getSourcePath().find(edit_from_object);
        if (from_root_attr != src_value)
        {
            // Use the relative path
            DataPath relative_path = new DataPath(from_root_attr, src_value);
            to_attr = relative_path.find(to_attr);
        }
        return to_attr;
    }
    
    /**
     * Removes the tuple from the mapped collection.
     * 
     * @param src_collection
     *            the source collection in the mapping.
     * @param tuple
     *            the tuple to remove.
     * @throws DataSourceException
     * @throws QueryExecutionException
     */
    public static void removeTuple(CollectionValue src_collection, TupleValue tuple)
            throws DataSourceException, QueryExecutionException
    {
        Value to_value = findTargetValueInMappedObject(src_collection);
        if (to_value == null) return;
        
        CollectionValue to_collection = (CollectionValue) to_value;
        // try to find this tuple
        boolean found = false;
        for (TupleValue to_tuple : new ArrayList<TupleValue>(to_collection.getTuples()))
        {
            if (ValueUtil.deepEquals(to_tuple, tuple))
            {
                found = true;
                to_collection.remove(to_tuple);
            }
        }
        assert found;
    }
    
    /**
     * Checks whether modifying the given src_value will modify the context.
     * 
     * @param src_value
     *            the source value.
     * @return whether the context will be modified.
     * @throws QueryExecutionException .
     * @throws DataSourceException .
     */
    public static boolean modifiesContext(Value src_value) throws QueryExecutionException, DataSourceException
    {
        UnifiedApplicationState uas = QueryProcessorFactory.getInstance().getUnifiedApplicationState();
        boolean w = !uas.isFplStackEmpty() && uas.getDataSource(DataSource.FPL_LOCAL_SCOPE).hasSchemaObject(DataSource.CONTEXT)
                && uas.getDataSource(DataSource.FPL_LOCAL_SCOPE).getDataObject(DataSource.CONTEXT) == src_value.getDataTree();
        
        return w;
    }
}
