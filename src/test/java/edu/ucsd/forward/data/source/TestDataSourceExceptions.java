/**
 * 
 */
package edu.ucsd.forward.data.source;

import java.util.Collections;
import java.util.Properties;

import org.testng.annotations.Test;
import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceMetaData.DataModel;
import edu.ucsd.forward.data.source.DataSourceMetaData.StorageSystem;
import edu.ucsd.forward.data.source.inmemory.InMemoryDataSource;
import edu.ucsd.forward.data.source.jdbc.JdbcDataSource;
import edu.ucsd.forward.data.source.jdbc.JdbcExclusion;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.exception.CheckedException;
import edu.ucsd.forward.exception.ExceptionMessages;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Tests data source errors.
 * 
 * @author Michalis Petropoulos
 * 
 */
@Test
public class TestDataSourceExceptions extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log      = Logger.getLogger(TestDataSourceExceptions.class);
    
    private static final String SOURCE   = "test_source";
    
    private static final String OBJ      = "test_obj";
    
    private static final String DRIVER   = "postgresql";
    
    private static final String HOST     = "localhost";
    
    private static final String PORT     = "5432";
    
    private static final String DATABASE = "forward";
    
    /**
     * Tests a data source exception message.
     * 
     * @throws DataSourceException
     *             exception.
     * @throws QueryExecutionException
     *             exception.
     */
    public void testInvalidNewSchemaObjName() throws DataSourceException, QueryExecutionException
    {
        CheckedException ex = null;
        DataSource data_source = null;
        try
        {
            data_source = new InMemoryDataSource(SOURCE, DataModel.SQLPLUSPLUS);
            
            data_source.open();
            data_source.createSchemaObject(OBJ, new SchemaTree(new TupleType()), Size.ONE);
            data_source.createSchemaObject(OBJ, new SchemaTree(new TupleType()), Size.ONE);
        }
        catch (QueryExecutionException exception)
        {
            ex = exception;
        }
        finally
        {
            data_source.close();
        }
        
        assertNotNull(ex);
        
        CheckedException ex1 = new DataSourceException(ExceptionMessages.DataSource.INVALID_NEW_SCHEMA_OBJ_NAME, OBJ, SOURCE);
        CheckedException ex2 = new QueryExecutionException(QueryExecution.CREATE_SCHEMA_OBJ_ERROR, ex1, OBJ, SOURCE);
        
        assertEquals(ex2.getSingleLineMessageWithLocation(), ex.getSingleLineMessageWithLocation());
    }
    
    /**
     * Tests a data source exception message.
     */
    public void testUnknownDataSourceName()
    {
        CheckedException ex = null;
        try
        {
            UnifiedApplicationState uas = new UnifiedApplicationState(Collections.<DataSource> emptyList());
            
            uas.getDataSource(SOURCE);
        }
        catch (DataSourceException exception)
        {
            ex = exception;
        }
        
        assertNotNull(ex);
        
        CheckedException ex1 = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_DATA_SRC_NAME, SOURCE);
        
        assertEquals(ex1.getSingleLineMessageWithLocation(), ex.getSingleLineMessageWithLocation());
    }
    
    /**
     * Tests a data source exception message.
     * 
     * @throws DataSourceException
     *             exception.
     * @throws QueryExecutionException
     *             exception.
     */
    public void testUnknownSchemaObjName() throws DataSourceException, QueryExecutionException
    {
        CheckedException ex = null;
        DataSource data_source = null;
        try
        {
            data_source = new InMemoryDataSource(SOURCE, DataModel.SQLPLUSPLUS);
            
            data_source.open();
            data_source.getSchemaObject(OBJ);
        }
        catch (QueryExecutionException exception)
        {
            ex = exception;
        }
        finally
        {
            data_source.close();
        }
        
        assertNotNull(ex);
        
        CheckedException ex1 = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_SCHEMA_OBJ_NAME, OBJ, SOURCE);
        CheckedException ex2 = new QueryExecutionException(QueryExecution.GET_SCHEMA_OBJ_ERROR, ex1, OBJ, SOURCE);
        
        assertEquals(ex2.getSingleLineMessageWithLocation(), ex.getSingleLineMessageWithLocation());
    }
    
    /**
     * Tests a data source exception message.
     * 
     * @throws DataSourceException
     *             exception.
     * @throws QueryExecutionException
     *             exception.
     */
    public void testUnknownDataObjName() throws DataSourceException, QueryExecutionException
    {
        CheckedException ex = null;
        DataSource data_source = null;
        try
        {
            data_source = new InMemoryDataSource(SOURCE, DataModel.SQLPLUSPLUS);
            
            data_source.open();
            data_source.getDataObject(OBJ);
        }
        catch (QueryExecutionException exception)
        {
            ex = exception;
        }
        finally
        {
            data_source.close();
        }
        
        assertNotNull(ex);
        
        CheckedException ex1 = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_DATA_OBJ_NAME, OBJ, SOURCE);
        CheckedException ex2 = new QueryExecutionException(QueryExecution.GET_DATA_OBJ_ERROR, ex1, OBJ, SOURCE);
        
        assertEquals(ex2.getSingleLineMessageWithLocation(), ex.getSingleLineMessageWithLocation());
    }
    
    /**
     * Tests a data source exception message.
     */
    public void testMissingAttr()
    {
        CheckedException ex = null;
        
        String relative_file_name = "TestDataSourceExceptions-testMissingAttr.xml";
        
        try
        {
            Element test_case_elm = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(relative_file_name));
            
            DataSourceXmlParser.parse(test_case_elm, new LocationImpl(relative_file_name));
        }
        catch (DataSourceConfigException exception)
        {
            ex = exception;
        }
        
        assertNotNull(ex);
        
        CheckedException ex1 = new DataSourceException(ExceptionMessages.DataSource.MISSING_ATTR, DataSourceXmlParser.NAME_ATTR);
        CheckedException ex2 = new DataSourceConfigException(ExceptionMessages.DataSource.CONFIG_ERROR,
                                                             new LocationImpl(relative_file_name), ex1, "");
        
        assertEquals(ex2.getSingleLineMessageWithLocation(), ex.getSingleLineMessageWithLocation());
    }
    
    /**
     * Tests a data source exception message.
     */
    public void testInvalidNewDataSrcName()
    {
        CheckedException ex = null;
        
        String relative_file_name = "TestDataSourceExceptions-testInvalidNewDataSrcName.xml";
        
        try
        {
            Element test_case_elm = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(relative_file_name));
            
            DataSourceXmlParser.parse(test_case_elm, new LocationImpl(relative_file_name));
        }
        catch (DataSourceConfigException exception)
        {
            ex = exception;
        }
        
        assertNotNull(ex);
        
        CheckedException ex1 = new DataSourceException(ExceptionMessages.DataSource.INVALID_NEW_DATA_SRC_NAME, SOURCE);
        CheckedException ex2 = new DataSourceConfigException(ExceptionMessages.DataSource.CONFIG_ERROR,
                                                             new LocationImpl(relative_file_name), ex1, SOURCE);
        
        assertEquals(ex2.getSingleLineMessageWithLocation(), ex.getSingleLineMessageWithLocation());
    }
    
    /**
     * Tests a data source exception message.
     */
    public void testUnknownDataModel()
    {
        CheckedException ex = null;
        try
        {
            DataModel.constantOf("which");
        }
        catch (DataSourceException exception)
        {
            ex = exception;
        }
        
        assertNotNull(ex);
        
        CheckedException ex1 = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_DATA_MODEL, "which");
        
        assertEquals(ex1.getSingleLineMessageWithLocation(), ex.getSingleLineMessageWithLocation());
    }
    
    /**
     * Tests a data source exception message.
     */
    public void testUnknownStorageSystem()
    {
        CheckedException ex = null;
        try
        {
            StorageSystem.constantOf("oops");
        }
        catch (DataSourceException exception)
        {
            ex = exception;
        }
        
        assertNotNull(ex);
        
        CheckedException ex1 = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_STORAGE_SYSTEM, "oops");
        
        assertEquals(ex1.getSingleLineMessageWithLocation(), ex.getSingleLineMessageWithLocation());
    }
    
    /**
     * Tests a data source exception message.
     */
    public void testUnknownCardinalityEstimate()
    {
        CheckedException ex = null;
        try
        {
            Size.constantOf("huge");
        }
        catch (DataSourceException exception)
        {
            ex = exception;
        }
        
        assertNotNull(ex);
        
        CheckedException ex1 = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_CARDINALITY_ESTIMATE, "huge");
        
        assertEquals(ex1.getSingleLineMessageWithLocation(), ex.getSingleLineMessageWithLocation());
    }
    
    /**
     * Tests a data source exception message.
     */
    public void testNoJdbcDriver()
    {
        CheckedException ex = null;
        try
        {
            Properties properties = new Properties();
            
            new JdbcDataSource(SOURCE, StorageSystem.JDBC, properties, false, Collections.<JdbcExclusion> emptyList());
        }
        catch (DataSourceException exception)
        {
            ex = exception;
        }
        catch (QueryExecutionException e)
        {
            assert (false);
        }
        
        assertNotNull(ex);
        
        CheckedException ex1 = new DataSourceException(ExceptionMessages.DataSource.NO_JDBC_DRIVER, SOURCE);
        
        assertEquals(ex1.getSingleLineMessageWithLocation(), ex.getSingleLineMessageWithLocation());
    }
    
    /**
     * Tests a data source exception message.
     */
    public void testNoJdbcProperties()
    {
        CheckedException ex = null;
        try
        {
            Properties properties = new Properties();
            
            properties.put("driver", DRIVER);
            properties.put("host", HOST);
            properties.put("port", PORT);
            properties.put("database", DATABASE);
            
            new JdbcDataSource(SOURCE, StorageSystem.JDBC, properties, false, Collections.<JdbcExclusion> emptyList());
        }
        catch (DataSourceException exception)
        {
            ex = exception;
        }
        catch (QueryExecutionException e)
        {
            assert (false);
        }
        
        assertNotNull(ex);
        
        CheckedException ex1 = new DataSourceException(ExceptionMessages.DataSource.NO_JDBC_PROPERTIES, SOURCE);
        
        assertEquals(ex1.getSingleLineMessageWithLocation(), ex.getSingleLineMessageWithLocation());
    }
    
    /**
     * Tests a data source exception message.
     */
    public void testInvalidJdbcModel()
    {
        CheckedException ex = null;
        
        String relative_file_name = "TestDataSourceExceptions-testInvalidJdbcModel.xml";
        
        try
        {
            Element test_case_elm = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(relative_file_name));
            
            DataSourceXmlParser.parse(test_case_elm, new LocationImpl(relative_file_name));
        }
        catch (DataSourceConfigException exception)
        {
            ex = exception;
        }
        
        assertNotNull(ex);
        
        CheckedException ex1 = new DataSourceException(ExceptionMessages.DataSource.INVALID_JDBC_MODEL, SOURCE);
        CheckedException ex2 = new DataSourceConfigException(ExceptionMessages.DataSource.CONFIG_ERROR,
                                                             new LocationImpl(relative_file_name), ex1, SOURCE);
        
        assertEquals(ex2.getSingleLineMessageWithLocation(), ex.getSingleLineMessageWithLocation());
    }
    
    /**
     * Tests a data source exception message.
     */
    public void testInvalidJdbcEnvironment()
    {
        CheckedException ex = null;
        
        String relative_file_name = "TestDataSourceExceptions-testInvalidJdbcEnvironment.xml";
        
        try
        {
            Element test_case_elm = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(relative_file_name));
            
            DataSourceXmlParser.parse(test_case_elm, new LocationImpl(relative_file_name));
        }
        catch (DataSourceConfigException exception)
        {
            ex = exception;
        }
        
        assertNotNull(ex);
        
        CheckedException ex1 = new DataSourceException(ExceptionMessages.DataSource.INVALID_JDBC_ENVIRONMENT, SOURCE, "production");
        CheckedException ex2 = new DataSourceConfigException(ExceptionMessages.DataSource.CONFIG_ERROR,
                                                             new LocationImpl(relative_file_name), ex1, SOURCE);
        
        assertEquals(ex2.getSingleLineMessageWithLocation(), ex.getSingleLineMessageWithLocation());
    }
    
    /**
     * Tests a data source exception message.
     */
    public void testJdbcPoolError()
    {
        CheckedException ex = null;
        try
        {
            Properties properties = new Properties();
            
            properties.put("driver", DRIVER);
            properties.put("host", HOST);
            properties.put("port", PORT);
            properties.put("database", DATABASE);
            
            properties.put("user", "who");
            properties.put("password", "what");
            
            new JdbcDataSource(SOURCE, StorageSystem.JDBC, properties, false, Collections.<JdbcExclusion> emptyList());
        }
        catch (DataSourceException exception)
        {
            ex = exception;
        }
        catch (QueryExecutionException exception)
        {
            ex = exception;
        }
        
        assertNotNull(ex);
        
        CheckedException ex1 = new DataSourceException(ExceptionMessages.DataSource.JDBC_POOL_ERROR, SOURCE);
        
        assert (ex.getSingleLineMessageWithLocation().startsWith(ex1.getSingleLineMessageWithLocation()));
    }
    
    /**
     * Tests a data source exception message.
     */
    public void testInvalidJdbcStorageSystem()
    {
        CheckedException ex = null;
        
        String relative_file_name = "TestDataSourceExceptions-testInvalidJdbcStorageSystem.xml";
        
        try
        {
            Element test_case_elm = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(relative_file_name));
            
            DataSourceXmlParser.parse(test_case_elm, new LocationImpl(relative_file_name));
        }
        catch (DataSourceConfigException exception)
        {
            ex = exception;
        }
        
        assertNotNull(ex);
        
        CheckedException ex1 = new DataSourceException(ExceptionMessages.DataSource.INVALID_JDBC_STORAGE_SYSTEM, SOURCE);
        CheckedException ex2 = new DataSourceConfigException(ExceptionMessages.DataSource.CONFIG_ERROR,
                                                             new LocationImpl(relative_file_name), ex1, "test_source");
        
        assertEquals(ex2.getSingleLineMessageWithLocation(), ex.getSingleLineMessageWithLocation());
    }
    
    /**
     * Tests a data source exception message.
     */
    public void testUnknownJdbcSchema()
    {
        CheckedException ex = null;
        
        try
        {
            Properties properties = new Properties();
            
            properties.put("driver", DRIVER);
            properties.put("host", HOST);
            properties.put("port", PORT);
            properties.put("database", DATABASE);
            properties.put("schema", "blah");
            
            properties.put("user", "postgres");
            properties.put("password", "postgres");
            
            new JdbcDataSource(SOURCE, StorageSystem.JDBC, properties, false, Collections.<JdbcExclusion> emptyList());
        }
        catch (DataSourceException exception)
        {
            ex = exception;
        }
        catch (QueryExecutionException e)
        {
            assert (false);
        }
        
        assertNotNull(ex);
        
        CheckedException ex1 = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_JDBC_SCHEMA, "blah", SOURCE);
        
        assertEquals(ex1.getSingleLineMessageWithLocation(), ex.getSingleLineMessageWithLocation());
    }
    
}
