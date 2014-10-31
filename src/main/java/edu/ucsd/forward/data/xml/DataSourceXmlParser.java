/**
 * 
 */
package edu.ucsd.forward.data.xml;

import static edu.ucsd.forward.xml.XmlUtil.getChildElements;
import static edu.ucsd.forward.xml.XmlUtil.getOnlyChildElement;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.index.IndexDeclaration;
import edu.ucsd.forward.data.index.IndexUtil;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceConfigException;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.DataSourceMetaData.DataModel;
import edu.ucsd.forward.data.source.DataSourceMetaData.Site;
import edu.ucsd.forward.data.source.DataSourceMetaData.StorageSystem;
import edu.ucsd.forward.data.source.inmemory.InMemoryDataSource;
import edu.ucsd.forward.data.source.remote.RemoteDataSource;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.exception.CheckedException;
import edu.ucsd.forward.exception.ExceptionMessages;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;

/**
 * The parser that parses an UAS.
 * 
 * @author Yupeng Fu
 * 
 */
public final class DataSourceXmlParser
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DataSourceXmlParser.class);
    
    /**
     * private constructor.
     */
    private DataSourceXmlParser()
    {
        
    }
    
    /**
     * Parses data sources and data objects.
     * 
     * @param element
     *            either a data source DOM element or the parent node of data source DOM elements.
     * @param location
     *            the location of the data source XML configuration file.
     * @return the parsed data sources
     * @throws DataSourceConfigException
     *             when parsing fails.
     */
    public static Collection<DataSource> parse(final Element element, Location location) throws DataSourceConfigException
    {
        Map<String, DataSource> data_sources = new LinkedHashMap<String, DataSource>();
        String source_name = null;
        
        List<Element> data_source_elms = getChildElements(element, DataSourceXmlSerializer.DATA_SOURCE_ELM);
        
        try
        {
            // Build input data sources
            for (Element source_elm : data_source_elms)
            {
                source_name = source_elm.getAttribute(DataSourceXmlSerializer.NAME_ATTR);
                
                DataSource data_source = parseDataSource(source_elm);
                
                if (data_source == null) continue;
                
                if (data_sources.containsKey(source_name))
                {
                    throw new DataSourceException(ExceptionMessages.DataSource.INVALID_NEW_DATA_SRC_NAME, source_name);
                }
                
                data_sources.put(source_name, data_source);
            }
            
            // Build schemas and data objects
            DataSourceXmlParser.parseDataObjects(element, data_sources);
        }
        catch (Exception ex)
        {
            // Chain the exception
            throw new DataSourceConfigException(ExceptionMessages.DataSource.CONFIG_ERROR, location, ex, source_name);
        }
        
        return data_sources.values();
    }
    
    /**
     * Parses a data source.
     * 
     * @param source_elm
     *            the DOM element of a data source.
     * @return a parsed data source.
     * @throws CheckedException
     *             if there is an error parsing a data source.
     */
    private static DataSource parseDataSource(Element source_elm) throws CheckedException
    {
        String source_name = source_elm.getAttribute(DataSourceXmlSerializer.NAME_ATTR);
        
        if (source_name.isEmpty())
        {
            throw new DataSourceException(ExceptionMessages.DataSource.MISSING_ATTR, DataSourceXmlSerializer.NAME_ATTR);
        }
        
        // Get the data model of the source
        String data_model_attr = source_elm.getAttribute(DataSourceXmlSerializer.DATA_MODEL_ATTR);
        
        // Get the storage system of the source
        String storage_system_attr = source_elm.getAttribute(DataSourceXmlSerializer.STORAGE_ATTR);
        StorageSystem storage_system = (!storage_system_attr.isEmpty())
                ? StorageSystem.constantOf(storage_system_attr)
                : StorageSystem.INMEMORY;
        
        // Get the optional site annotation
        String site_attr = source_elm.getAttribute(DataSourceXmlSerializer.SITE_ATTR);
        Site site = (site_attr.isEmpty()) ? Site.SERVER : Site.constantOf(site_attr);
        
        if (storage_system == StorageSystem.INMEMORY)
        {
            // The default data model for in-memory data source is SQL++
            DataModel data_model = (data_model_attr != null) ? DataModel.constantOf(data_model_attr) : DataModel.SQLPLUSPLUS;
            DataSource data_source = new InMemoryDataSource(source_name, data_model, site);
            
            return data_source;
        }
        else
        {
            throw new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_STORAGE_SYSTEM, source_name);
        }
    }
    
    /**
     * Parses a data object.
     * 
     * @param parent_elm
     *            the parent DOM element of data objects.
     * @param data_sources
     *            the existing data sources.
     * @throws Exception
     *             if there is an error parsing a data object.
     */
    public static void parseDataObjects(Element parent_elm, Map<String, DataSource> data_sources) throws Exception
    {
        for (Element object_elm : getChildElements(parent_elm, DataSourceXmlSerializer.DATA_OBJECT_ELM))
        {
            // Create or retrieve related data source
            String source_name = object_elm.getAttribute(DataSourceXmlSerializer.EXECUTION_ATTR);
            if (source_name.isEmpty())
            {
                throw new DataSourceException(ExceptionMessages.DataSource.MISSING_ATTR, DataSourceXmlSerializer.EXECUTION_ATTR);
            }
            
            DataSource data_source = null;
            
            // A shorthand for creating in-memory data source
            // Intended only for test cases and not for production
            if (!data_sources.containsKey(source_name))
            {
                // Get the data model of the source
                String data_model_attr = object_elm.getAttribute(DataSourceXmlSerializer.DATA_MODEL_ATTR);
                DataModel data_model = (!data_model_attr.isEmpty())
                        ? DataModel.constantOf(data_model_attr)
                        : DataModel.SQLPLUSPLUS;
                
                // Get the storage system of the source
                String storage_system_attr = object_elm.getAttribute(DataSourceXmlSerializer.STORAGE_ATTR);
                StorageSystem storage_system = (!storage_system_attr.isEmpty())
                        ? StorageSystem.constantOf(storage_system_attr)
                        : StorageSystem.INMEMORY;
                
                assert (storage_system == StorageSystem.INMEMORY);
                
                // Get the optional site annotation
                String site_attr = object_elm.getAttribute(DataSourceXmlSerializer.SITE_ATTR);
                Site site = (site_attr.isEmpty()) ? Site.SERVER : Site.constantOf(site_attr);
                
                data_source = new InMemoryDataSource(source_name, data_model, site);
                
                data_sources.put(source_name, data_source);
            }
            else data_source = data_sources.get(source_name);
            
            // Build schema tree
            Element schema_tree_elm = getOnlyChildElement(object_elm, TypeXmlSerializer.SCHEMA_TREE_ELM);
            SchemaTree schema_tree = TypeXmlParser.parseSchemaTree(schema_tree_elm);
            
            // Build data tree
            Element data_tree_elm = getOnlyChildElement(object_elm, ValueXmlSerializer.DATA_TREE_ELM);
            DataTree data_tree = ValueXmlParser.parseDataTree(data_tree_elm, schema_tree);
            
            // Add schema and data objects to data source
            String cardinality = object_elm.getAttribute(DataSourceXmlSerializer.ESTIMATE_ATTR);
            Size size = (!cardinality.isEmpty()) ? Size.constantOf(cardinality) : Size.SMALL;
            
            String data_obj_name = object_elm.getAttribute(DataSourceXmlSerializer.NAME_ATTR);
            
            data_source.open();
            data_source.createSchemaObject(data_obj_name, schema_tree, size);
            if (!(data_source instanceof RemoteDataSource)) data_source.setDataObject(data_obj_name, data_tree);
            // Build indices
            for (IndexDeclaration declaration : IndexUtil.getIndexDeclarations(schema_tree))
            {
                // Remove the index
                declaration.getCollectionType().removeIndex(declaration.getName());
                data_source.createIndex(data_obj_name, declaration.getName(), declaration.getCollectionTypeSchemaPath(),
                                        declaration.getKeyPaths(), declaration.isUnique(), declaration.getMethod());
            }
            data_source.close();
        }
    }
    
}
