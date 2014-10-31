/**
 * 
 */
package edu.ucsd.forward.data.source;

import static edu.ucsd.forward.xml.XmlUtil.getAttributeNames;
import static edu.ucsd.forward.xml.XmlUtil.getChildElements;
import static edu.ucsd.forward.xml.XmlUtil.getOnlyChildElement;
import static edu.ucsd.forward.xml.XmlUtil.getOptionalChildElement;
import static java.lang.String.format;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.xml.sax.SAXParseException;

import com.google.gwt.xml.client.CharacterData;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.config.Config;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.constraint.ForeignKeyConstraint;
import edu.ucsd.forward.data.index.IndexDeclaration;
import edu.ucsd.forward.data.index.IndexUtil;
import edu.ucsd.forward.data.source.DataSourceMetaData.DataModel;
import edu.ucsd.forward.data.source.DataSourceMetaData.Site;
import edu.ucsd.forward.data.source.DataSourceMetaData.StorageSystem;
import edu.ucsd.forward.data.source.idb.IndexedDbDataSource;
import edu.ucsd.forward.data.source.inmemory.InMemoryDataSource;
import edu.ucsd.forward.data.source.jdbc.JdbcDataSource;
import edu.ucsd.forward.data.source.jdbc.JdbcEagerResult;
import edu.ucsd.forward.data.source.jdbc.JdbcExclusion;
import edu.ucsd.forward.data.source.jdbc.JdbcExecution;
import edu.ucsd.forward.data.source.jdbc.JdbcTransaction;
import edu.ucsd.forward.data.source.jdbc.statement.GenericStatement;
import edu.ucsd.forward.data.source.remote.RemoteDataSource;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.xml.DataSourceXmlSerializer;
import edu.ucsd.forward.data.xml.TypeXmlParser;
import edu.ucsd.forward.data.xml.TypeXmlSerializer;
import edu.ucsd.forward.data.xml.ValueXmlParser;
import edu.ucsd.forward.data.xml.ValueXmlSerializer;
import edu.ucsd.forward.exception.CheckedException;
import edu.ucsd.forward.exception.ExceptionMessages;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.util.xml.XmlParserException;
import edu.ucsd.forward.xml.DomApiProxy;
import edu.ucsd.forward.xml.ElementProxy;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * An abstract implementation of the query processor test cases.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 */
public final class DataSourceXmlParser
{
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(DataSourceXmlParser.class);
    
    public static final String  DATA_SOURCE_ELM  = "data_source";
    
    public static final String  DATA_OBJECT_ELM  = "data_object";
    
    public static final String  NAME_ATTR        = "name";
    
    public static final String  OVERWRITE_ATTR   = "overwrite";
    
    public static final String  PROPERTIES_ELM   = "properties";
    
    public static final String  ESTIMATE_ATTR    = "cardinality_estimate";
    
    public static final String  DATA_MODEL_ATTR  = "data_model";
    
    public static final String  STORAGE_ATTR     = "storage_system";
    
    public static final String  EXECUTION_ATTR   = "execution_data_source";
    
    public static final String  QUERY_EXPR_ELM   = "query_expression";
    
    public static final String  ENVIRONMENT_ATTR = "environment";
    
    public static final String  HOST_ATTR        = "host";
    
    public static final String  LOCALHOST_VAL    = "localhost";
    
    public static final String  EXCLUDE_ELM      = "exclude";
    
    /**
     * Hidden constructor.
     */
    private DataSourceXmlParser()
    {
    }
    
    public static Collection<DataSource> parse(String xml_string, String name) throws DataSourceConfigException
    {
        ElementProxy element = null;
        try
        {
            element = (ElementProxy) XmlUtil.parseDomNode(xml_string);
        }
        catch (RuntimeException e)
        {
            if (e.getCause() instanceof SAXParseException)
            {
                throw new DataSourceConfigException(ExceptionMessages.DataSource.SAX_EXCEPTION, new LocationImpl(name),
                                                    e.getCause());
            }
        }
        
        return parse(element, new LocationImpl(name));
    }
    
    /**
     * Parses data sources and data objects.
     * 
     * @param element
     *            either a data source DOM element or the parent node of data source DOM elements.
     * @param location
     *            the location of the data source XML configuration file.
     * @return the collection of parsed data sources.
     * @throws DataSourceConfigException
     *             if there is an error configuring a data source.
     */
    public static Collection<DataSource> parse(Element element, Location location) throws DataSourceConfigException
    {
        Map<String, DataSource> data_sources = new HashMap<String, DataSource>();
        String source_name = null;
        
        List<Element> data_source_elms = null;
        if (element.getTagName().equals(DATA_SOURCE_ELM))
        {
            data_source_elms = Collections.singletonList(element);
        }
        else
        {
            data_source_elms = getChildElements(element, DATA_SOURCE_ELM);
        }
        
        try
        {
            // Build input data sources
            for (Element source_elm : data_source_elms)
            {
                source_name = source_elm.getAttribute(NAME_ATTR);
                
                DataSource data_source = DataSourceXmlParser.parseDataSource(source_elm);
                
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
        String source_name = source_elm.getAttribute(NAME_ATTR);
        
        if (source_name.isEmpty())
        {
            throw new DataSourceException(ExceptionMessages.DataSource.MISSING_ATTR, NAME_ATTR);
        }
        
        // Get the data model of the source
        String data_model_attr = source_elm.getAttribute(DATA_MODEL_ATTR);
        
        // Get the storage system of the source
        String storage_system_attr = source_elm.getAttribute(STORAGE_ATTR);
        StorageSystem storage_system = (!storage_system_attr.isEmpty())
                ? StorageSystem.constantOf(storage_system_attr)
                : StorageSystem.INMEMORY;
        
        DataModel data_model;
        DataSource data_source;
        switch (storage_system)
        {
            case INMEMORY:
                // The default data model for in-memory data source is SQL++
                data_model = (!data_model_attr.isEmpty()) ? DataModel.constantOf(data_model_attr) : DataModel.SQLPLUSPLUS;
                // Get the optional site annotation
                String site_attr = source_elm.getAttribute(DataSourceXmlSerializer.SITE_ATTR);
                Site site = (site_attr.isEmpty()) ? Site.SERVER : Site.constantOf(site_attr);
                
                data_source = new InMemoryDataSource(source_name, data_model, site);
                
                return data_source;
            case INDEXEDDB:
                data_model = (!data_model_attr.isEmpty()) ? DataModel.constantOf(data_model_attr) : DataModel.RELATIONAL;
                data_source = new IndexedDbDataSource(source_name, data_model);
                
                return data_source;
            case REMOTE:
                // The default data model for remote data source is SQL++
                data_model = (!data_model_attr.isEmpty()) ? DataModel.constantOf(data_model_attr) : DataModel.SQLPLUSPLUS;
                data_source = new RemoteDataSource(source_name, data_model);
                return data_source;
            case JDBC:
                // The default data model for JDBC data source is relational
                data_model = (!data_model_attr.isEmpty()) ? DataModel.constantOf(data_model_attr) : DataModel.RELATIONAL;
                
                if (data_model != DataModel.RELATIONAL)
                {
                    throw new DataSourceException(ExceptionMessages.DataSource.INVALID_JDBC_MODEL, source_name);
                }
                
                // Get exclusions
                List<JdbcExclusion> exclusions = new ArrayList<JdbcExclusion>();
                Element exclude_elm = getOptionalChildElement(source_elm, EXCLUDE_ELM);
                if (exclude_elm != null)
                {
                    for (Element child_elm : getChildElements(exclude_elm))
                    {
                        if (child_elm.getTagName().equals("foreign-key"))
                        {
                            JdbcExclusion exclusion = new JdbcExclusion(JdbcExclusion.Type.FOREIGN_KEY);
                            exclusions.add(exclusion);
                            for (String attr_name : getAttributeNames(child_elm))
                            {
                                exclusion.getProperties().put(attr_name, child_elm.getAttribute(attr_name));
                            }
                        }
                        else
                        {
                            throw new UnsupportedOperationException();
                        }
                    }
                }
                
                // Get connection properties
                Set<String> seen_environments = new LinkedHashSet<String>();
                for (Element properties_elm : getChildElements(source_elm, PROPERTIES_ELM))
                {
                    Properties connection_properties = new Properties();
                    
                    // Check host
                    String host = properties_elm.getAttribute(HOST_ATTR);
                    boolean flag = Config.getAllowRemoteDatabaseConnections();
                    if (!host.toLowerCase().equals(LOCALHOST_VAL) && !flag)
                    {
                        throw new DataSourceException(ExceptionMessages.DataSource.REMOTE_CONNECTIONS_NOT_ALLOWED);
                    }
                    
                    // Check environment
                    String environment = properties_elm.getAttribute(ENVIRONMENT_ATTR);
                    environment = environment.toLowerCase();
                    if (seen_environments.contains(environment))
                    {
                        throw new DataSourceException(ExceptionMessages.DataSource.INVALID_JDBC_ENVIRONMENT, source_name,
                                                      environment);
                    }
                    seen_environments.add(environment);
                    if (!environment.equals(Config.getEnvironment().toLowerCase())) continue;
                    
                    // Check if importing the existing database
                    String overwrite_attr = properties_elm.getAttribute(OVERWRITE_ATTR);
                    boolean overwrite = (!overwrite_attr.isEmpty()) ? Boolean.parseBoolean(overwrite_attr) : false;
                    
                    for (String attr_name : getAttributeNames(properties_elm))
                    {
                        connection_properties.put(attr_name, properties_elm.getAttribute(attr_name));
                    }
                    
                    data_source = new JdbcDataSource(source_name, storage_system, connection_properties, overwrite, exclusions);
                    
                    return data_source;
                }
                
                throw new DataSourceException(ExceptionMessages.DataSource.MISSING_JDBC_ENVIRONMENT, source_name,
                                              Config.getEnvironment().toLowerCase());
            default:
                throw new AssertionError();
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
        // Accumulate foreign key constraint elements
        List<Element> foreign_key_constraint_elms = new ArrayList<Element>();
        
        for (Element object_elm : getChildElements(parent_elm, DATA_OBJECT_ELM))
        {
            // Create or retrieve related data source
            String source_name = object_elm.getAttribute(EXECUTION_ATTR);
            if (source_name.isEmpty())
            {
                throw new DataSourceException(ExceptionMessages.DataSource.MISSING_ATTR, EXECUTION_ATTR);
            }
            
            DataSource data_source = null;
            
            // A shorthand for creating in-memory data source
            // Intended only for test cases and not for production
            if (!data_sources.containsKey(source_name))
            {
                // Get the data model of the source
                String data_model_attr = object_elm.getAttribute(DATA_MODEL_ATTR);
                DataModel data_model = (!data_model_attr.isEmpty())
                        ? DataModel.constantOf(data_model_attr)
                        : DataModel.SQLPLUSPLUS;
                
                // Get the storage system of the source
                String storage_system_attr = object_elm.getAttribute(STORAGE_ATTR);
                StorageSystem storage_system = (!storage_system_attr.isEmpty())
                        ? StorageSystem.constantOf(storage_system_attr)
                        : StorageSystem.INMEMORY;
                        
                // Get the optional site annotation
                String site_attr = object_elm.getAttribute(DataSourceXmlSerializer.SITE_ATTR);
                Site site = (site_attr.isEmpty()) ? Site.SERVER : Site.constantOf(site_attr);
                        
                
                assert (storage_system == StorageSystem.INMEMORY);
                
                data_source = new InMemoryDataSource(source_name, data_model, site);
                
                data_sources.put(source_name, data_source);
            }
            else data_source = data_sources.get(source_name);
            
            // Build schema tree
            Element schema_tree_elm = getOnlyChildElement(object_elm, TypeXmlSerializer.SCHEMA_TREE_ELM);
            SchemaTree schema_tree = TypeXmlParser.parseSchemaTree(schema_tree_elm, foreign_key_constraint_elms);
            
            // Build data tree
            Element data_tree_elm = getOnlyChildElement(object_elm, ValueXmlSerializer.DATA_TREE_ELM);
            DataTree data_tree = parseDataTree(data_tree_elm, schema_tree, data_sources);
            
            // Add schema and data objects to data source
            String cardinality = object_elm.getAttribute(ESTIMATE_ATTR);
            Size size = (!cardinality.isEmpty()) ? Size.constantOf(cardinality) : Size.SMALL;
            
            String data_obj_name = object_elm.getAttribute(NAME_ATTR);
            
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
        
        // Parse foreign key constraints after all data objects have been parsed.
        for (Element constraint_elm : foreign_key_constraint_elms)
            parseForeignKeyConstraint(constraint_elm, data_sources);
        
    }
    
    /**
     * Parses a foreign key constraint, which comes with a collection type.
     * 
     * @param constraint_element
     *            a foreign key constraint element of an XML DOM.
     * @param data_sources
     *            the existing data sources.
     * @throws XmlParserException
     *             if an error occurs during conversion.
     */
    public static void parseForeignKeyConstraint(Element constraint_element, Map<String, DataSource> data_sources)
            throws XmlParserException
    {
        String attribute_path;
        SchemaPath path;
        
        List<SchemaObjectHandle> handles = new ArrayList<SchemaObjectHandle>();
        List<CollectionType> collection_types = new ArrayList<CollectionType>();
        List<List<ScalarType>> attr_lists = new ArrayList<List<ScalarType>>();
        
        for (int i = 0; i < 2; i++)
        {
            Element elm = getOnlyChildElement(constraint_element, (i == 0)
                    ? TypeXmlSerializer.FOREIGN_ELM
                    : TypeXmlSerializer.PRIMARY_ELM);
            
            // Retrieve the schema object handle to the collection type
            handles.add(new SchemaObjectHandle(elm.getAttribute(DataSourceXmlParser.DATA_SOURCE_ELM),
                                               elm.getAttribute(DataSourceXmlParser.DATA_OBJECT_ELM)));
            
            // Retrieve the collection type
            if (!elm.hasAttribute(TypeXmlSerializer.PATH_ATTR))
            {
                String s = format("Element <%s> needs to have an attribute <%s>", (i == 0)
                        ? TypeXmlSerializer.FOREIGN_ELM
                        : TypeXmlSerializer.PRIMARY_ELM, TypeXmlSerializer.PATH_ATTR);
                throw new XmlParserException(s);
            }
            
            attribute_path = elm.getAttribute(TypeXmlSerializer.PATH_ATTR);
            try
            {
                path = new SchemaPath(attribute_path);
            }
            catch (ParseException e)
            {
                String s = format("Invalid schema path: <%s>", attribute_path);
                throw new XmlParserException(s, e);
            }
            
            SchemaTree schema = null;
            try
            {
                schema = data_sources.get(handles.get(i).getDataSourceName()).getSchemaObject(handles.get(i).getSchemaObjectName()).getSchemaTree();
            }
            catch (QueryExecutionException e)
            {
                String s = format("Error accessing data source");
                throw new XmlParserException(s, e);
            }
            
            Type collection_type = path.find(schema);
            if (collection_type == null || !(collection_type instanceof CollectionType))
            {
                String s = format("No collection type found at path: <%s>", attribute_path);
                throw new XmlParserException(s);
            }
            collection_types.add((CollectionType) collection_type);
            
            // Retrieve the paths to the attributes of scalar type
            List<ScalarType> attr_list = new ArrayList<ScalarType>();
            for (Element child : getChildElements(elm, TypeXmlSerializer.ATTRIBUTE_ELM))
            {
                if (!child.hasAttribute(TypeXmlSerializer.PATH_ATTR))
                {
                    String s = format("Element <%s> needs to have an attribute <%s>", TypeXmlSerializer.ATTRIBUTE_ELM,
                                      TypeXmlSerializer.PATH_ATTR);
                    throw new XmlParserException(s);
                }
                
                attribute_path = child.getAttribute(TypeXmlSerializer.PATH_ATTR);
                try
                {
                    path = new SchemaPath(attribute_path);
                }
                catch (ParseException e)
                {
                    String s = format("Invalid schema path: <%s>", attribute_path);
                    throw new XmlParserException(s, e);
                }
                Type attr_type = path.find(collection_type);
                
                if (attr_type == null || !(attr_type instanceof ScalarType))
                {
                    String s = format("No scalar type found at path: <%s>", attribute_path);
                    throw new XmlParserException(s);
                }
                
                attr_list.add((ScalarType) attr_type);
            }
            attr_lists.add(attr_list);
            
            if (attr_list.size() == 0)
            {
                String s = format("Element <%s> needs to have at least one <%s> subelement", (i == 0)
                        ? TypeXmlSerializer.FOREIGN_ELM
                        : TypeXmlSerializer.PRIMARY_ELM, TypeXmlSerializer.ATTRIBUTE_ELM);
                throw new XmlParserException(s);
            }
            
            // Check that the closest ancestor collection type of all key attributes is the same one
            for (Type type : attr_list)
            {
                CollectionType closest_relation_type = type.getClosestAncestor(CollectionType.class);
                if (closest_relation_type == null || closest_relation_type != collection_type)
                {
                    // Could not display more information regarding which collection it is because at this point the collection type
                    // is
                    // detached.
                    String s = format("The attributes of <%s> do not have the same closest ancestor collection type", (i == 0)
                            ? TypeXmlSerializer.FOREIGN_ELM
                            : TypeXmlSerializer.PRIMARY_ELM);
                    throw new XmlParserException(s);
                }
            }
        }
        
        new ForeignKeyConstraint(handles.get(0), collection_types.get(0), attr_lists.get(0), handles.get(1),
                                 collection_types.get(1), attr_lists.get(1));
    }
    
    /**
     * Parses a data tree from an XML element. The data tree could be specified directly or using a query that retrieves values from
     * a JDBC data source.
     * 
     * @param data_tree_elm
     *            the DOM element specifying the data tree.
     * @param schema_tree
     *            the schema of the data tree.
     * @param data_sources
     *            the existing data sources.
     * @return the parsed data tree.
     * @throws Exception
     *             if there is an error parsing the data tree.
     */
    public static DataTree parseDataTree(Element data_tree_elm, SchemaTree schema_tree, Map<String, DataSource> data_sources)
            throws Exception
    {
        
        DataTree data_tree = null;
        if (getOptionalChildElement(data_tree_elm, QUERY_EXPR_ELM) != null)
        {
            CollectionType collection_type = (CollectionType) schema_tree.getRootType();
            Element query_expr_elm = getOnlyChildElement(data_tree_elm, QUERY_EXPR_ELM);
            String query_expr = ((CharacterData) query_expr_elm.getChildNodes().item(0)).getData();
            
            String src_name = query_expr_elm.getAttribute(EXECUTION_ATTR);
            
            if (!data_sources.containsKey(src_name))
            {
                throw new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_DATA_SRC_NAME, src_name);
            }
            
            StorageSystem storage = data_sources.get(src_name).getMetaData().getStorageSystem();
            if (storage != StorageSystem.JDBC)
            {
                throw new DataSourceException(ExceptionMessages.DataSource.INVALID_JDBC_STORAGE_SYSTEM, src_name);
            }
            JdbcDataSource data_source = (JdbcDataSource) data_sources.get(src_name);
            
            data_source.open();
            
            JdbcTransaction transaction = data_source.beginTransaction();
            
            GenericStatement statement = new GenericStatement(query_expr);
            JdbcEagerResult result = JdbcExecution.run(transaction, statement);
            CollectionValue collection = new CollectionValue();
            Binding binding = result.next();
            
            Object[] attribute_names = collection_type.getTupleType().getAttributeNames().toArray();
            
            while (binding != null)
            {
                TupleValue tuple = new TupleValue();
                for (int i = 0; i < collection_type.getTupleType().getSize(); i++)
                {
                    tuple.setAttribute((String) attribute_names[i], binding.getValue(i).getValue());
                }
                collection.add(tuple);
                binding = result.next();
            }
            
            transaction.commit();
            
            data_source.close();
            
            data_tree = new DataTree(collection);
        }
        else
        {
            data_tree = ValueXmlParser.parseDataTree(data_tree_elm, schema_tree);
        }
        
        return data_tree;
    }
    
}
