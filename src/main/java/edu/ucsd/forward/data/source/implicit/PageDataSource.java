package edu.ucsd.forward.data.source.implicit;

import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.DataSourceMetaData.DataModel;
import edu.ucsd.forward.data.source.SchemaObject;
import edu.ucsd.forward.data.source.inmemory.InMemoryDataSource;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.query.QueryExecutionException;

/**
 * Represents the implicit page data source that has page scope. Page scope refers to the duration a page instance is displayed on a
 * specific window, during a specific session of a specific application. During the page scope, the browser can send AJAX requests
 * to the server, but cannot refresh the entire contents of the window and cannot send a GET or PUT request.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class PageDataSource extends InMemoryDataSource
{
    /**
     * Constructs an empty page data source.
     */
    public PageDataSource()
    {
        super(getName(), DataModel.SQLPLUSPLUS);
    }
    
    /**
     * Constructs an empty page data source.
     * 
     * @param source_name
     *            the name of the page data source.
     */
    public PageDataSource(String source_name)
    {
        super(source_name, DataModel.SQLPLUSPLUS);
    }
    
    /**
     * Constructs a page data source by coping and initializing persistent schema objects from an initial page data source.
     * 
     * @param initial_data_source
     *            The initial data source to copy schema trees from.
     * @throws DataSourceException
     *             if opening or closing the data source fails.
     * @throws QueryExecutionException
     *             if a transaction fails.
     */
    public PageDataSource(PageDataSource initial_data_source) throws DataSourceException, QueryExecutionException
    {
        this();
        
        // Open the data source
        this.open();
        
        // Copy to the page data source the page data objects defined in DDL files.
        // FIXME This is currently required because we allow defining the page data object in a separate DDL file,
        // instead of having the definition of the prolog of the page configuration.
        for (SchemaObject schema_object : initial_data_source.getSchemaObjects())
        {
            String name = schema_object.getName();
            SchemaTree schema_tree = schema_object.getSchemaTree();
            
            this.createSchemaObject(name, schema_tree, schema_object.getCardinalityEstimate());
        }
        
        // Close the data source
        this.close();
    }
    
    /**
     * Checks if the schema object is defined by the framework in the page data source.
     * 
     * @param schema_object
     *            schema object
     * @return boolean
     */
    public static boolean isFrameworkSchemaObject(SchemaObject schema_object)
    {
        return schema_object.getName().equals(SchemaObject.MUTABLE_SCHEMA_OBJECT)
                || schema_object.getName().equals(SchemaObject.PAGE_SCHEMA_OBJECT)
                || schema_object.getName().equals(SchemaObject.VISUAL_SCHEMA_OBJECT);
    }
    
    /**
     * Returns the data source name.
     * 
     * @return the data source name.
     */
    public static String getName()
    {
        return DataSource.PAGE;
    }
}
