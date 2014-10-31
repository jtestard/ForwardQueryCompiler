/**
 * 
 */
package edu.ucsd.forward.data.xml;

import static edu.ucsd.forward.xml.XmlUtil.createChildElement;

import com.google.gwt.xml.client.Element;
import com.google.gwt.xml.client.Node;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.SchemaObject;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.query.QueryExecutionException;

/**
 * The serializer that serializes the data sources.
 * 
 * @author Yupeng Fu
 * 
 */
public final class DataSourceXmlSerializer
{
    @SuppressWarnings("unused")
    private static final Logger   log              = Logger.getLogger(DataSourceXmlSerializer.class);
    
    protected static final String UAS_ELM          = "uas";
    
    protected static final String NAME_ATTR        = "name";
    
    protected static final String EXECUTION_ATTR   = "execution_data_source";
    
    public static final String    DATA_SOURCE_ELM  = "data_source";
    
    public static final String    DATA_OBJECT_ELM  = "data_object";
    
    public static final String    OVERWRITE_ATTR   = "overwrite";
    
    public static final String    PROPERTIES_ELM   = "properties";
    
    public static final String    ESTIMATE_ATTR    = "cardinality_estimate";
    
    public static final String    STORAGE_ATTR     = "storage_system";
    
    public static final String    ENVIRONMENT_ATTR = "environment";
    
    public static final String    DATA_MODEL_ATTR  = "data_model";
    
    public static final String    SITE_ATTR        = "site";
    
    /**
     * Constructor.
     */
    private DataSourceXmlSerializer()
    {
        
    }
    
    /**
     * Serialize the given UAS.
     * 
     * @param uas
     *            the uas
     * @param parent_node
     *            the parent node
     * @throws QueryExecutionException
     *             anything wrong happens
     */
    public static void serializeUas(UnifiedApplicationState uas, Node parent_node) throws QueryExecutionException
    {
        for (String ds_name : uas.getDataSourceNames())
        {
            if (ds_name.equals(DataSource.MEDIATOR) || ds_name.equals(DataSource.FPL_LOCAL_SCOPE)
                    || ds_name.equals(DataSource.TEMP_ASSIGN_SOURCE)) continue;
            
            DataSource ds = null;
            try
            {
                ds = uas.getDataSource(ds_name);
            }
            catch (DataSourceException e)
            {
                throw new RuntimeException(e);
            }
            
            // Serialize data source.
            Element ds_node = createChildElement(parent_node, DATA_SOURCE_ELM);
            ds_node.setAttribute(DATA_MODEL_ATTR, ds.getMetaData().getDataModel().name());
            ds_node.setAttribute(STORAGE_ATTR, ds.getMetaData().getStorageSystem().name());
            ds_node.setAttribute(NAME_ATTR, ds.getMetaData().getName());
            ds_node.setAttribute(SITE_ATTR, ds.getMetaData().getSite().name());
            
            // Serialize data objects
            for (SchemaObject so : ds.getSchemaObjects())
            {
                Element do_node = createChildElement(parent_node, DATA_OBJECT_ELM);
                
                do_node.setAttribute(NAME_ATTR, so.getName());
                do_node.setAttribute(EXECUTION_ATTR, ds_name);
                TypeXmlSerializer.serializeSchemaTree(so.getSchemaTree(), do_node);
                ValueXmlSerializer.serializeDataTree(ds.getDataObject(so.getName()), do_node);
            }
        }
    }
}
