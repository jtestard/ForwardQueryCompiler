package edu.ucsd.forward.data.source;

import edu.ucsd.forward.exception.ExceptionMessages;

/**
 * Data source meta data stores the name of the data source, schema objects and various other attributes.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class DataSourceMetaData
{
    /**
     * The name of the data source.
     */
    private String m_name;
    
    /**
     * An enumeration of data models accommodated by the query engine.
     */
    public enum DataModel
    {
        RELATIONAL, SQLPLUSPLUS, ADM;
        
        /**
         * Returns the data model constant with the specified name.
         * 
         * @param name
         *            the name of the constant to return.
         * @return the data model constant with the specified name.
         * @throws DataSourceException
         *             if there is no data model constant with the specified name.
         */
        public static DataModel constantOf(String name) throws DataSourceException
        {
            try
            {
                return DataModel.valueOf(name);
            }
            catch (IllegalArgumentException ex)
            {
                throw new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_DATA_MODEL, name);
            }
        }
    }
    
    /**
     * The data model of the data source.
     */
    private DataModel m_data_model;
    
    /**
     * An enumeration of data source storage systems.
     */
    public enum StorageSystem
    {
        INMEMORY, JDBC, INDEXEDDB, REMOTE, ASTERIX;
        
        /**
         * Returns the storage system constant with the specified name.
         * 
         * @param name
         *            the name of the constant to return.
         * @return the storage system constant with the specified name.
         * @throws DataSourceException
         *             if there is no storage system constant with the specified name.
         */
        public static StorageSystem constantOf(String name) throws DataSourceException
        {
            try
            {
                return StorageSystem.valueOf(name);
            }
            catch (IllegalArgumentException ex)
            {
                throw new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_STORAGE_SYSTEM, name);
            }
        }
    }
    
    /**
     * The storage system of the data source.
     */
    private StorageSystem m_storage_system;
    
    /**
     * An enumeration of data source sites.
     */
    public enum Site
    {
        CLIENT, SERVER;
        
        /**
         * Returns the site constant with the specified name.
         * 
         * @param name
         *            the name of the constant to return.
         * @return the site constant with the specified name.
         * @throws DataSourceException
         *             if there is no site constant with the specified name.
         */
        public static Site constantOf(String name) throws DataSourceException
        {
            try
            {
                return Site.valueOf(name);
            }
            catch (IllegalArgumentException ex)
            {
                throw new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_SITE, name);
            }
        }
    }
    
    /**
     * The site of the data source.
     */
    private Site m_site;
    
    /**
     * Constructs an instance of a data source meta data with the Site set to SERVER by default.
     * 
     * @param name
     *            name of the data source.
     * @param model
     *            the model of the data source.
     * @param storage_system
     *            the storage system of the data source.
     */
    public DataSourceMetaData(String name, DataModel model, StorageSystem storage_system)
    {
        this(name, model, storage_system, Site.SERVER);
    }
    
    /**
     * Constructs an instance of a data source meta data.
     * 
     * @param name
     *            name of the data source.
     * @param model
     *            the model of the data source.
     * @param storage_system
     *            the storage system of the data source.
     * @param site
     *            the site of the data source.
     */
    public DataSourceMetaData(String name, DataModel model, StorageSystem storage_system, Site site)
    {
        assert (name != null);
        assert (model != null);
        assert (storage_system != null);
        assert (site != null);
        
        if (model == DataModel.SQLPLUSPLUS)
        {
            assert (storage_system == StorageSystem.INMEMORY || storage_system == StorageSystem.REMOTE);
        }
        
        m_name = name;
        m_data_model = model;
        m_storage_system = storage_system;
        m_site = site;
    }
    
    /**
     * Returns the name of the data source.
     * 
     * @return the data source name.
     */
    public String getName()
    {
        return m_name;
    }
    
    /**
     * Returns the data model implemented by the data source.
     * 
     * @return a data model.
     */
    public DataModel getDataModel()
    {
        return m_data_model;
    }
    
    /**
     * Returns the storage system used by the data source.
     * 
     * @return a storage system.
     */
    public StorageSystem getStorageSystem()
    {
        return m_storage_system;
    }
    
    /**
     * Returns the storage system used by the data source.
     * 
     * @return a storage system.
     */
    public Site getSite()
    {
        return m_site;
    }
    
}
