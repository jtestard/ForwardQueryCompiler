/**
 * 
 */
package edu.ucsd.forward.data.source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.mapping.MappingGroup;
import edu.ucsd.forward.data.source.DataSource.DataSourceState;
import edu.ucsd.forward.data.source.DataSourceMetaData.DataModel;
import edu.ucsd.forward.data.source.DataSourceMetaData.Site;
import edu.ucsd.forward.data.source.implicit.PageDataSource;
import edu.ucsd.forward.data.source.implicit.RequestDataSource;
import edu.ucsd.forward.data.source.implicit.SessionDataSource;
import edu.ucsd.forward.data.source.inmemory.InMemoryDataSource;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.exception.ExceptionMessages;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;

/**
 * The unified application state stores and retrieves data sources containing schema and data objects used by the programs and pages
 * of the application. There is a single unified application state per application per Java Virtual Machine. The unified application
 * state opens when the corresponding application starts and closes when the application stops.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class UnifiedApplicationState
{
    @SuppressWarnings("unused")
    private static final Logger log       = Logger.getLogger(UnifiedApplicationState.class);
    
    public static final String  DUMMY_APP = "dummyApp";
    
    /**
     * An enumeration of UAS states.
     */
    public enum State
    {
        OPEN, CLOSED;
    }
    
    /**
     * The state of the UAS.
     */
    private State                     m_state;
    
    /**
     * The data sources.
     */
    private Map<String, DataSource>   m_data_sources;
    
    /**
     * The special data source for FPL local storage.
     */
    private Stack<InMemoryDataSource> m_fpl_stack;
    
    /**
     * The mapping groups that maps an object in source to another.
     */
    private List<MappingGroup>        m_mapping_groups;
    
    /**
     * Construct a unified application state for the input data sources.
     * 
     * @param data_sources
     *            a list of data sources.
     */
    public UnifiedApplicationState(Collection<DataSource> data_sources)
    {
        assert (data_sources != null);
        
        m_data_sources = new LinkedHashMap<String, DataSource>();
        m_fpl_stack = new Stack<InMemoryDataSource>();
        
        for (DataSource data_src : data_sources)
        {
            String name = data_src.getMetaData().getName();
            if (name.equals(DataSource.MEDIATOR) || name.equals(DataSource.FPL_LOCAL_SCOPE)
                    || name.equals(DataSource.TEMP_ASSIGN_SOURCE)) continue;
            m_data_sources.put(name, data_src);
        }
        
        // Add the mediator data source
        m_data_sources.put(DataSource.MEDIATOR, new InMemoryDataSource(DataSource.MEDIATOR, DataModel.SQLPLUSPLUS));
        
        // Add the client-side mediator data source
        m_data_sources.put(DataSource.CLIENT, new InMemoryDataSource(DataSource.MEDIATOR, DataModel.SQLPLUSPLUS, Site.CLIENT));
        
        // Add the temp assign data source
        m_data_sources.put(DataSource.TEMP_ASSIGN_SOURCE, new InMemoryDataSource(DataSource.TEMP_ASSIGN_SOURCE,
                                                                                 DataModel.SQLPLUSPLUS));
        
        m_state = State.CLOSED;
        m_mapping_groups = new ArrayList<MappingGroup>();
    }
    
    /**
     * Adds one mapping group.
     * 
     * @param group
     *            mapping group to add.
     */
    public void addMappingGroup(MappingGroup group)
    {
        m_mapping_groups.add(group);
    }
    
    /**
     * Get the mapping groups.
     * 
     * @return the mapping groups.
     */
    public List<MappingGroup> getMappingGroups()
    {
        return m_mapping_groups;
    }
    
    /**
     * Clear all the mapping groups.
     */
    public void clearMappingGroups()
    {
        m_mapping_groups.clear();
    }
    
    /**
     * Requests a new FPL local storage frame. Important: it's the caller's responsibility to guarantee there is no more than one
     * concurrent action execution.
     * 
     * @throws QueryExecutionException
     *             if the FPL source fails to open.
     */
    public void requestNewFplSource() throws QueryExecutionException
    {
        InMemoryDataSource fpl_source = new InMemoryDataSource(DataSource.FPL_LOCAL_SCOPE, DataModel.SQLPLUSPLUS);
        fpl_source.open();
        m_fpl_stack.push(fpl_source);
    }
    
    /**
     * Checks if the FPL local storage stack is empty.
     * 
     * @return if the FPL local storage stack is empty.
     */
    public boolean isFplStackEmpty()
    {
        return m_fpl_stack.isEmpty();
    }
    
    /**
     * Pop the top FPL local storage frame.
     * 
     * @throws QueryExecutionException
     *             if the top FPL source fails to close.
     */
    public void removeTopFplSource() throws QueryExecutionException
    {
        InMemoryDataSource fpl_source = m_fpl_stack.pop();
        fpl_source.close();
    }
    
    /**
     * Adds one more data source to the UAS. The UAS has to be closed. If the data source is open, then an exception is thrown.
     * 
     * @param data_source
     *            the data source to add.
     * @throws QueryExecutionException
     *             if the UAS is closed and the data source is open.
     */
    public void addDataSource(DataSource data_source) throws QueryExecutionException
    {
        assert (m_state == State.CLOSED);
        assert (data_source != null);
        String name = data_source.getMetaData().getName();
        if (data_source.getState() == DataSourceState.CLOSED && m_state == State.OPEN) data_source.open();
        try
        {
            if (data_source.getState() == DataSourceState.OPEN && m_state == State.CLOSED)
            {
                throw new DataSourceException(edu.ucsd.forward.exception.ExceptionMessages.DataSource.INVALID_OPEN_STATE, name);
            }
        }
        catch (DataSourceException e)
        {
            throw new QueryExecutionException(QueryExecution.DATA_SOURCE_ACCESS, e);
        }
        m_data_sources.put(name, data_source);
    }
    
    /**
     * Removes a data source from the UAS. The UAS has to be closed. If the data source has outstanding transactions, then an
     * exception is thrown.
     * 
     * @param name
     *            the name of the data source to remove.
     * @throws QueryExecutionException
     *             if the data source does not exist, or the data source has outstanding transactions.
     */
    public void removeDataSource(String name) throws QueryExecutionException
    {
        assert (m_state == State.CLOSED);
        assert (name != null);
        if (!m_data_sources.containsKey(name))
        {
            throw new QueryExecutionException(QueryExecution.DATA_SOURCE_ACCESS,
                                              new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_DATA_SRC_NAME, name));
        }
        m_data_sources.remove(name);
    }
    
    /**
     * Gets the state of the UAS.
     * 
     * @return the state of the UAS.
     */
    public State getState()
    {
        return m_state;
    }
    
    /**
     * Opens all data sources in the unified application state.
     * 
     * @exception QueryExecutionException
     *                if opening the data sources raises an exception.
     */
    public void open() throws QueryExecutionException
    {
        assert (m_state == State.CLOSED);
        
        for (DataSource data_source : m_data_sources.values())
        {
            if (data_source.getState() == DataSourceState.CLOSED) data_source.open();
        }
        
        m_state = State.OPEN;
        
        if (!m_fpl_stack.isEmpty())
        {
            throw new QueryExecutionException(QueryExecution.DATA_SOURCE_ACCESS,
                                              new DataSourceException(ExceptionMessages.DataSource.UNCLEANED_UP_FPL_STACK));
        }
    }
    
    /**
     * Closes all data sources in the unified application state.
     * 
     * @exception QueryExecutionException
     *                if closing the data sources raises an exception.
     */
    public void close() throws QueryExecutionException
    {
        if (m_state == State.CLOSED) return;
        
        // First cleanup the query processor so that DataSourceAccesses are removed along with active transactions
        QueryProcessorFactory.getInstance().cleanup(true);
        
        // Then close the UAS
        for (DataSource data_source : m_data_sources.values())
        {
            if (data_source.getState() == DataSourceState.OPEN) data_source.close();
        }
        
        if (!m_fpl_stack.isEmpty())
        {
            throw new QueryExecutionException(QueryExecution.DATA_SOURCE_ACCESS,
                                              new DataSourceException(ExceptionMessages.DataSource.UNCLEANED_UP_FPL_STACK));
        }
        
        m_state = State.CLOSED;
    }
    
    /**
     * Closes all request, page and session data sources in the unified application state.
     * 
     * @exception QueryExecutionException
     *                if closing the data sources raises an exception.
     */
    public void closeTransient() throws QueryExecutionException
    {
        for (DataSource data_source : m_data_sources.values())
        {
            if (data_source instanceof RequestDataSource || data_source instanceof PageDataSource
                    || data_source instanceof SessionDataSource)
            {
                if (data_source.getState() == DataSourceState.OPEN) data_source.close();
            }
        }
        
        m_state = State.CLOSED;
    }
    
    /**
     * Returns the set of data sources in unified application state.
     * 
     * @return the set of data sources in unified application state.
     */
    public Collection<DataSource> getDataSources()
    {
        return Collections.<DataSource> unmodifiableCollection(m_data_sources.values());
    }
    
    /**
     * Returns the set of data object names in unified application state.
     * 
     * @return the set of data object names in unified application state.
     */
    public Set<String> getDataSourceNames()
    {
        return Collections.<String> unmodifiableSet(m_data_sources.keySet());
    }
    
    /**
     * Determines whether the unified application state has a data source with the given name.
     * 
     * @param name
     *            the name of the data source.
     * @return true, if the unified application state has a data source with the given name; false, otherwise.
     */
    public boolean hasDataSource(String name)
    {
        return name.equals(DataSource.FPL_LOCAL_SCOPE) || m_data_sources.containsKey(name);
    }
    
    /**
     * Gets the data source with the given name.
     * 
     * @param name
     *            the name of the data source.
     * @return a data source.
     * @exception DataSourceException
     *                if a data source with the given name does not exist in the unified application state.
     */
    public DataSource getDataSource(String name) throws DataSourceException
    {
        if (name.equals(DataSource.FPL_LOCAL_SCOPE))
        {
            if (m_fpl_stack.isEmpty())
            {
                throw new DataSourceException(ExceptionMessages.DataSource.EMPTY_FPL_STACK);
            }
            return m_fpl_stack.peek();
        }
        
        if (name.equals(DataSource.PAGE))
        {
            DataSource page_source = m_data_sources.get(DataSource.PAGE);
            if (page_source != null)
            {
                return page_source;
            }
        }
        else if (this.hasDataSource(name))
        {
            return m_data_sources.get(name);
        }
        
        throw new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_DATA_SRC_NAME, name);
    }
    
    @Override
    public String toString()
    {
        StringBuilder b = new StringBuilder();
        for (DataSource source : this.getDataSources())
        {
            b.append(source.toString());
            b.append("\n");
        }
        b.append("The FPL storage stack:");
        for (InMemoryDataSource frame : m_fpl_stack)
        {
            b.append(frame.toString());
            b.append("\n");
        }
        return b.toString();
    }
    
    /**
     * Creates a schema object and sets its data object to the unified application state. If there are existing objects with the
     * same name, they are overwritten.
     * 
     * @param data_source_name
     *            data source name
     * @param data_object_name
     *            data object name
     * @param schema_tree
     *            schema tree
     * @param estimate
     *            estimate
     * @param data_tree
     *            data tree
     * @throws QueryExecutionException
     *             if a transaction fails.
     */
    public void addSchemaAndData(String data_source_name, String data_object_name, SchemaTree schema_tree, Size estimate,
            DataTree data_tree) throws QueryExecutionException
    {
        DataSource data_source = null;
        try
        {
            data_source = this.getDataSource(data_source_name);
        }
        catch (DataSourceException ex)
        {
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.DATA_SOURCE_ACCESS, ex, data_source_name);
        }
        
        // Drop existing schema object and delete existing data object
        if (data_source.hasSchemaObject(data_object_name))
        {
            data_source.dropSchemaObject(data_object_name);
        }
        
        // Create new schema object and set its data object
        data_source.createSchemaObject(data_object_name, schema_tree, estimate);
        data_source.setDataObject(data_object_name, data_tree);
    }
    
}
