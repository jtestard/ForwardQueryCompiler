package edu.ucsd.forward.query;

import edu.ucsd.forward.data.source.DataSourceMetaData;

/**
 * Determines whether a class is compatible with a given data source.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface DataSourceCompliance
{
    
    /**
     * Determines whether a SQL++ query construct can be evaluated by the given data source.
     * 
     * @param metadata
     *            the data source to consider.
     * @return true, if the given data source can evaluate a SQL++ query construct; false, otherwise.
     */
    public boolean isDataSourceCompliant(DataSourceMetaData metadata);
    
}
