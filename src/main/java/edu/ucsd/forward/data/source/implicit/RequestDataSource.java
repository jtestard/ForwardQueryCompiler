package edu.ucsd.forward.data.source.implicit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.DataSourceMetaData.DataModel;
import edu.ucsd.forward.data.source.SchemaObject;
import edu.ucsd.forward.data.source.inmemory.InMemoryDataSource;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;
import edu.ucsd.forward.util.NameGenerator;

/**
 * Represents the implicit request data source that has request scope. Request scope coincides with the duration of an HTTP request,
 * issued from a specific page on a specific window, during a specific session of a specific application.
 * 
 * @author Michalis Petropoulos
 * @author Erick Zamora
 */
public class RequestDataSource extends InMemoryDataSource
{
    @SuppressWarnings("unused")
    private static final Logger log                               = Logger.getLogger(RequestDataSource.class);
    
    public static final String  NAME                              = "request";
    
    public static final String  ENVIRONMENT_DATA_OBJECT           = "environment";
    
    public static final String  ENVIRONMENT_TUPLE_NAME_ATTRIBUTE  = "name";
    
    public static final String  ENVIRONMENT_TUPLE_VALUE_ATTRIBUTE = "value";
    
    public static final String  USER_AGENT                        = "user_agent";
    
    public static final String  RAW_URL                           = "raw_url";
    
    public static final String  RAW_URL_PARAMETERS                = "parameters";
    
    public static final String  RAW_URL_ID                        = "id";
    
    public static final String  RAW_URL_NAME                      = "name";
    
    public static final String  RAW_URL_VALUE                     = "value";
    
    public static final String  NEXT_PAGE_PATH_DATA_OBJECT        = NameGenerator.NAME_PREFIX + "next_page_path";
    
    public static final String  REDIRECT_TARGET_DATA_OBJECT       = NameGenerator.NAME_PREFIX + "redirect_target";
    
    public static final String  REDIRECT_TARGET_INPUT_DATA_OBJECT = NameGenerator.NAME_PREFIX + "redirect_target_input";
    
    public static final String  REDIRECT_NEW_WINDOW_DATA_OBJECT   = NameGenerator.NAME_PREFIX + "redirect_new_window";
    
    public static final String  REDIRECT_METHOD_DATA_OBJECT       = NameGenerator.NAME_PREFIX + "redirect_method";
    
    public static final String  IS_RESUME_REFRESH_DATA_OBJECT     = NameGenerator.NAME_PREFIX + "is_resume_refresh";
    
    public static final String  IS_RESUME_INIT_DATA_OBJECT        = NameGenerator.NAME_PREFIX + "is_resume_init";
    
    /**
     * Constructs a request data source with no data.
     */
    public RequestDataSource()
    {
        super(NAME, DataModel.SQLPLUSPLUS);
        
        // Add the user_agent schema object
        try
        {
            this.open();
            this.createSchemaObject(USER_AGENT, new SchemaTree(new StringType()), Size.ONE);
            this.close();
        }
        catch (QueryExecutionException e)
        {
            // This should never happen
            throw new AssertionError();
        }
    }
    
    /**
     * Constructs an instance of an implicit request data source.
     * 
     * @param request
     *            The HTTP request corresponding to this implicit request data source.
     * @throws QueryExecutionException
     *             if a transaction fails.
     * @throws DataSourceException
     *             if opening or closing the data source fails.
     */
    public RequestDataSource(HttpServletRequest request) throws QueryExecutionException, DataSourceException
    {
        this();
        
        assert (request != null);
        
        // Open the data source
        this.open();
        
        // Set the user_agent data object
        String ua = request.getHeader("User-Agent");
        this.setDataObject(USER_AGENT, new DataTree((ua != null) ? new StringValue(ua) : new NullValue()));
        
        // Close the data source
        this.close();
    }
    
    @Override
    public Collection<SchemaObject> getSchemaObjects()
    {
        Collection<SchemaObject> objs = super.getSchemaObjects();
        
        // Take out user agent
        // FIXME: Why does the user agent need to be omitted?
        List<SchemaObject> out_objs = new ArrayList<SchemaObject>();
        for (SchemaObject obj : objs)
        {
            if (obj.getName().equals(USER_AGENT)) continue;
            out_objs.add(obj);
        }
        
        return Collections.unmodifiableCollection(out_objs);
    }
    
    /**
     * Returns true if the schema object is the requet.environment data object.
     * 
     * @param schema_obj
     *            schema object
     * @return boolean
     */
    public static boolean isEnvironmentSchemaObject(SchemaObject schema_obj)
    {
        return ENVIRONMENT_DATA_OBJECT.equals(schema_obj.getName());
    }
    
    /**
     * Returns the raw url schema tree.
     * 
     * @return the raw url schema tree.
     */
    public static SchemaTree getRawUrlSchemaTree()
    {
        TupleType raw_url_type = new TupleType();
        CollectionType parameters_type = new CollectionType();
        TupleType parameters_entry = new TupleType();
        parameters_entry.setAttribute(RAW_URL_ID, new IntegerType());
        parameters_entry.setAttribute(RAW_URL_NAME, new StringType());
        parameters_entry.setAttribute(RAW_URL_VALUE, new StringType());
        
        parameters_type.setTupleType(parameters_entry);
        raw_url_type.setAttribute(RAW_URL_PARAMETERS, parameters_type);
        
        return new SchemaTree(raw_url_type);
    }
}
