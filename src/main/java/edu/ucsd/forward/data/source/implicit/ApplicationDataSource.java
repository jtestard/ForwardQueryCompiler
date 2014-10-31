package edu.ucsd.forward.data.source.implicit;

import javax.servlet.ServletContext;

import edu.ucsd.forward.data.source.DataSourceMetaData.DataModel;
import edu.ucsd.forward.data.source.inmemory.InMemoryDataSource;

/**
 * Represents the implicit application data source that has application scope.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class ApplicationDataSource extends InMemoryDataSource
{
    public static final String NAME                       = "application";
    
    public static final String APP_DEBUG_MODE_DATA_OBJECT = "app_debug_mode";
    
    public static final String APP_DEBUG_LOG_DATA_OBJECT  = "app_debug_log";
    
    private ServletContext     m_servlet_context;
    
    /**
     * Constructs an application data source with no data.
     */
    public ApplicationDataSource()
    {
        super(NAME, DataModel.SQLPLUSPLUS);
    }
    
    /**
     * Constructs an instance of an implicit application data source.
     * 
     * @param servlet_context
     *            The servlet context corresponding to this implicit application data source.
     */
    public ApplicationDataSource(ServletContext servlet_context)
    {
        super(NAME, DataModel.SQLPLUSPLUS);
        
        assert (servlet_context != null);
        m_servlet_context = servlet_context;
        
        // Add this implicit application data source to the servlet context as an attribute
        m_servlet_context.setAttribute(this.getMetaData().getName(), this);
    }
    
}
