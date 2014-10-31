package edu.ucsd.forward.data.source.implicit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import javax.servlet.http.HttpSession;

import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.DataSourceTransaction;
import edu.ucsd.forward.data.source.SchemaObject;
import edu.ucsd.forward.data.source.DataSourceMetaData.DataModel;
import edu.ucsd.forward.data.source.inmemory.InMemoryDataSource;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;

/**
 * Represents the implicit session data source that has session scope. Session scope coincides with the duration of an HTTP session
 * of a specific application.
 * 
 * @author Kian Win
 * 
 */
public class SessionDataSource extends InMemoryDataSource
{
    public static final String NAME                          = "session";
    
    public static final String HTTP_SESSION_ID               = "session_id";
    
    public static final String HTTP_SESSION_ATTRIBUTE_PREFIX = "forward_session.session_data_sources.";
    
    private HttpSession        m_session;
    
    /**
     * Constructs a session data source with no data.
     */
    public SessionDataSource()
    {
        super(NAME, DataModel.SQLPLUSPLUS);
        
        // Add the session_id schema object
        try
        {
            this.open();
            this.createSchemaObject(HTTP_SESSION_ID, new SchemaTree(new StringType()), Size.ONE);
            this.close();
        }
        catch (QueryExecutionException e)
        {
            // This should never happen
            throw new AssertionError();
        }
    }
    
    /**
     * Constructs an instance of an implicit session data source.
     * 
     * @param session
     *            The HTTP session corresponding to this implicit session data source.
     * @param initial_data_source
     *            The initial data source to copy the schema tree from if the HTTP session has not been initialized.
     * @throws QueryExecutionException
     *             if a transaction fails.
     * @throws DataSourceException
     *             if opening or closing the data source fails.
     */
    public SessionDataSource(HttpSession session, SessionDataSource initial_data_source)
            throws QueryExecutionException, DataSourceException
    {
        this();
        
        assert (session != null);
        m_session = session;
        
        // Open the data source
        this.open();
        
        // Set the session_id data object
        this.setDataObject(HTTP_SESSION_ID, new DataTree(new StringValue(m_session.getId())));
        
        List<String> attribute_names = getAttributeNames(session);
        
        if (attribute_names.size() == 0)
        {
            // HTTP session has not been initialized
            
            for (SchemaObject schema_object : initial_data_source.getSchemaObjects())
            {
                String name = schema_object.getName();
                SchemaTree schema_tree = schema_object.getSchemaTree();
                
                // Skip if it is the session_id.
                if (name.equals(HTTP_SESSION_ID)) continue;
                
                createSchemaObject(name, schema_tree, schema_object.getCardinalityEstimate());
            }
        }
        
        else
        {
            // Populate the data source with data trees that exist in the underlying HTTP session
            for (String attribute_name : getAttributeNames(session))
            {
                if (!attribute_name.startsWith(HTTP_SESSION_ATTRIBUTE_PREFIX)) continue;
                SessionDataObjectEntry entry = (SessionDataObjectEntry) m_session.getAttribute(attribute_name);
                SchemaObject schema_object = entry.getSchemaObject();
                DataTree data_tree = entry.getDataObjectTree();
                String data_object_name = attribute_name.substring(HTTP_SESSION_ATTRIBUTE_PREFIX.length());
                assert (data_object_name.equals(schema_object.getName()));
                
                createSchemaObject(data_object_name, schema_object.getSchemaTree(), schema_object.getCardinalityEstimate());
                super.setDataObject(data_object_name, data_tree);
            }
        }
        
        // Close the data source
        this.close();
    }
    
    @Override
    public Collection<SchemaObject> getSchemaObjects()
    {
        Collection<SchemaObject> objs = super.getSchemaObjects();
        
        // Take out session_id
        List<SchemaObject> out_objs = new ArrayList<SchemaObject>();
        for (SchemaObject obj : objs)
        {
            if (obj.getName().equals(HTTP_SESSION_ID)) continue;
            out_objs.add(obj);
        }
        
        return Collections.unmodifiableCollection(out_objs);
    }
    
    @Override
    public void setDataObject(String name, DataTree data_tree, DataSourceTransaction transaction) throws QueryExecutionException
    {
        super.setDataObject(name, data_tree, transaction);
        SchemaObject schema_object = this.getSchemaObject(name);
        
        // Write the data tree and schema tree into the underlying HTTP session, unless it is the initialization data tree in which
        // case the session has not been set yet, or it is the session_id.
        if (m_session != null && !name.equals(HTTP_SESSION_ID))
        {
            m_session.setAttribute(HTTP_SESSION_ATTRIBUTE_PREFIX + name, new SessionDataObjectEntry(data_tree, schema_object));
        }
    }
    
    /**
     * Returns the attribute names in a HTTP session.
     * 
     * @param session
     *            the HTTP session.
     * @return the attribute names.
     */
    @SuppressWarnings("unchecked")
    private static List<String> getAttributeNames(HttpSession session)
    {
        List<String> attribute_names = new ArrayList<String>();
        for (Enumeration<String> e = session.getAttributeNames(); e.hasMoreElements();)
        {
            String attribute_name = e.nextElement();
            
            if (!attribute_name.startsWith(HTTP_SESSION_ATTRIBUTE_PREFIX)) continue;
            
            attribute_names.add(attribute_name);
        }
        return attribute_names;
    }
    
    /**
     * Returns the HttpSession.
     * @return the HttpSession
     */
    public HttpSession getSession()
    {
        return m_session;
    }
    
}
