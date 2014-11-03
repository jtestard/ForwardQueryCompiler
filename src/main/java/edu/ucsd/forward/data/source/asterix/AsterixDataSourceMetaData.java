/**
 * 
 */
package edu.ucsd.forward.data.source.asterix;

import java.util.Properties;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.source.DataSourceMetaData.StorageSystem;

/**
 * @author Jules Testard
 *
 */
public class AsterixDataSourceMetaData extends DataSourceMetaData
{
    public static final String DRIVER   = "driver";
    
    public static final String HOST     = "host";
    
    public static final String PORT     = "port";
    
    public static final String DATABASE = "database";
    
    /**
     * The ASTERIX connection URL.
     */
    private String             m_url;
    
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AsterixDataSourceMetaData.class);
    
    /**
     * @param name
     * @param properties
     */
    public AsterixDataSourceMetaData(String name, Properties properties)
    {
        super(name, DataModel.ADM, StorageSystem.ASTERIX);
        String host = properties.getProperty(HOST);
        String port = properties.getProperty(PORT);
        m_url = String.format("http://%s:%s/", host, port);
    }

    /**
     * Returns the connection URL of the ASTERIX data source.
     * 
     * @return the connection URL
     */
    public String getConnectionUrl()
    {
        return m_url;
    }
    
}
