/**
 * 
 */
package edu.ucsd.forward.data.mapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.ValueTypeMapUtil;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * A mapping group is a group of mappings. Currently mapping group is constructed per action call site and is used for building the
 * context state. Therefore all the source and target names are fixed.
 * 
 * 
 * @author Yupeng Fu
 * 
 */
public class MappingGroup
{
    @SuppressWarnings("unused")
    private static final Logger  log = Logger.getLogger(MappingGroup.class);
    
    // FIMXE: Add action call site here.
    
    private List<Mapping>        m_mappings;
    
    private String               m_from_source;
    
    private String               m_from_object;
    
    private String               m_to_source;
    
    private String               m_to_object;
    
    private PhysicalPlan         m_context_plan;
    
    private SchemaTree           m_context_schema;
    
    private SchemaTree           m_event_schema;
    
    /**
     * The index of mappings from data path to the attribute in index.
     */
    private Map<String, Mapping> m_src_idx;
    
    /**
     * Constructor.
     */
    public MappingGroup()
    {
        m_mappings = new ArrayList<Mapping>();
        m_src_idx = new HashMap<String, Mapping>();
        // All the hard-coded from and to sources
        m_to_source = DataSource.PAGE;
        m_to_object = "page";
        m_from_source = DataSource.FPL_LOCAL_SCOPE;
        m_from_object = DataSource.CONTEXT;
    }
    
    /**
     * Sets the context plan.
     * 
     * @param plan
     *            the context plan
     */
    public void setContextPlan(PhysicalPlan plan)
    {
        assert plan != null;
        m_context_plan = plan;
        
        // Because all context plans are evaluated within actions, we set the nested property to true.
        // This indicates that the logical plan of the context is nested within the logical plan
        // of whatever action is being called. This is necessary because the QueryProcessor,
        // when executing createLazyQueryResult, checks the nested property. If nested is false
        // and the query processor has open transactions, createLazyQueryResult will rollback.
        m_context_plan.getLogicalPlan().setNested(true);
    }
    
    /**
     * Evaluates the mapping query to reset the from data.
     * 
     * @throws QueryExecutionException .
     * @throws DataSourceException .
     */
    public void evaluateMappingQuery() throws QueryExecutionException, DataSourceException
    {
        UnifiedApplicationState uas = QueryProcessorFactory.getInstance().getUnifiedApplicationState();
        if (m_context_plan == null || !uas.getDataSource(m_from_source).hasSchemaObject(m_from_object)) return;
        
        Value new_value = QueryProcessorFactory.getInstance().createEagerQueryResult(m_context_plan, uas).getValue();
        DataTree new_value_tree = new DataTree(new_value);
        uas.getDataSource(m_from_source).setDataObject(m_from_object, new_value_tree);
        
        ValueTypeMapUtil.map(new_value_tree, m_context_schema);
    }
    
    /**
     * Given the unified application state, evaluates the mapping query to reset the from data.
     * 
     * @param uas
     *            the unified application state.
     * @throws QueryExecutionException .
     * @throws DataSourceException .
     */
    public void evaluateMappingQuery(UnifiedApplicationState uas) throws QueryExecutionException, DataSourceException
    {
        if (m_context_plan == null || !uas.getDataSource(m_from_source).hasSchemaObject(m_from_object)) return;
        Value new_value = QueryProcessorFactory.getInstance().createEagerQueryResult(m_context_plan, uas).getValue();
        
        DataTree new_value_tree = new DataTree(new_value);
        uas.getDataSource(m_from_source).setDataObject(m_from_object, new_value_tree);
        ValueTypeMapUtil.map(new_value_tree, m_context_schema);
    }
    
    /**
     * Gets the mappings.
     * 
     * @return the mappings.
     */
    public List<Mapping> getMappings()
    {
        return m_mappings;
    }
    
    /**
     * Adds a mapping.
     * 
     * @param mapping
     *            the mapping to add.
     */
    public void addMapping(Mapping mapping)
    {
        assert mapping != null;
        m_mappings.add(mapping);
        m_src_idx.put(mapping.getSourcePath().toString(), mapping);
    }
    
    /**
     * Gets the mapping given the data path to the source attribute.
     * 
     * @param src_path
     *            the data path to the source attribute in string.
     * @return the matching mapping.
     */
    public Mapping getMapping(String src_path)
    {
        return m_src_idx.get(src_path);
    }
    
    /**
     * Instantiates all the mappings with the event state.
     * 
     * @param event_state
     *            the event state.
     */
    public void instantiate(DataTree event_state)
    {
        for (Mapping mapping : getMappings())
        {
            mapping.instantiate(event_state);
        }
    }
    
    /**
     * Clears the mappings.
     */
    public void clear()
    {
        m_mappings.clear();
    }
    
    /**
     * Gets the from source.
     * 
     * @return the from source.
     */
    public String getFromSource()
    {
        return m_from_source;
    }
    
    /**
     * Gets the to source.
     * 
     * @return the to source.
     */
    public String getToSource()
    {
        return m_to_source;
    }
    
    /**
     * Gets the from object.
     * 
     * @return the from object.
     */
    public String getFromObject()
    {
        return m_from_object;
    }
    
    /**
     * Gets the to object.
     * 
     * @return the to object.
     */
    public String getToObject()
    {
        return m_to_object;
    }
    
    /**
     * Gets the context schema.
     * 
     * @return the context schema.
     */
    public SchemaTree getContextSchema()
    {
        return m_context_schema;
    }
    
    /**
     * Sets the context schema.
     * 
     * @param context_schema
     *            the context schema.
     */
    public void setContextSchema(SchemaTree context_schema)
    {
        assert (context_schema != null);
        m_context_schema = context_schema;
    }
    
    /**
     * Gets the event schema.
     * 
     * @return the event schema.
     */
    public SchemaTree getEventSchema()
    {
        return m_event_schema;
    }
    
    /**
     * Sets the event schema.
     * 
     * @param event_schema
     *            the event schema.
     */
    public void setEventSchema(SchemaTree event_schema)
    {
        assert (event_schema != null);
        m_event_schema = event_schema;
    }
}
