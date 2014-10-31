package edu.ucsd.forward.data.source.jdbc;

import java.util.Properties;

import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.exception.ExceptionMessages;

/**
 * JDBC Data source meta data.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class JdbcDataSourceMetaData extends DataSourceMetaData
{
    public static final String SCHEMA   = "schema";
    
    public static final String DRIVER   = "driver";
    
    public static final String HOST     = "host";
    
    public static final String PORT     = "port";
    
    public static final String DATABASE = "database";
    
    /**
     * The JDBC connection URL.
     */
    private String             m_url;
    
    /**
     * The schema name.
     */
    private String             m_schema;
    
    /**
     * Constructs an instance of a JDBC data source meta data.
     * 
     * @param name
     *            name of the data source.
     * @param storage_system
     *            the storage system of the data source.
     * @param properties
     *            the connection properties of the JDBC data source.
     * @throws DataSourceException
     *             when the right properties are not provided.
     */
    public JdbcDataSourceMetaData(String name, StorageSystem storage_system, Properties properties) throws DataSourceException
    {
        super(name, DataModel.RELATIONAL, storage_system);
        
        String schema = properties.getProperty(SCHEMA);
        String driver = properties.getProperty(DRIVER);
        String host = properties.getProperty(HOST);
        String port = properties.getProperty(PORT);
        String database = properties.getProperty(DATABASE);
        
        m_url = toConnectionUrl(driver, host, port, database);
        
        m_schema = (schema == null) ? name : schema;
    }
    
    /**
     * Returns the connection URL of the JDBC data source.
     * 
     * @return the connection URL.
     */
    protected String getConnectionUrl()
    {
        return m_url;
    }
    
    /**
     * Returns the name of the schema.
     * 
     * @return the name of the schema.
     */
    public String getSchemaName()
    {
        return m_schema;
    }
    
    /**
     * Outputs the connection URL from the specification.
     * 
     * @param driver
     *            the name of the JDBC driver of the JDBC data source.
     * @param host
     *            the host name of the database.
     * @param port
     *            the port of the host for the database.
     * @param database
     *            the name of the database.
     * @return a connection URL.
     * @throws DataSourceException
     *             when the right properties are not provided.
     */
    private String toConnectionUrl(String driver, String host, String port, String database) throws DataSourceException
    {
        if (driver == null || driver.isEmpty() || host == null || port == null || database == null)
        {
            throw new DataSourceException(ExceptionMessages.DataSource.NO_JDBC_DRIVER, this.getName());
        }
        
        /*-
         * Example connection strings:
         * - jdbc:postgresql:database
         * - jdbc:postgresql://host/database
         * - jdbc:postgresql://host:port/database (unsupported)
         * - jdbc:h2:~/database
         * - jdbc:h2:mem:
         * - jdbc:h2:mem:database
         * - jdbc:h2:tcp://localhost/~/database
         */
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:");
        sb.append(driver);
        sb.append(":");
        if (!host.isEmpty())
        {
            sb.append("//");
            sb.append(host);
            sb.append(":");
            sb.append(port);
            sb.append("/");
        }
        if (!database.isEmpty())
        {
            sb.append(database);
        }
        return sb.toString();
    }
}
