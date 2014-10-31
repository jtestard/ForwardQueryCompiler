package edu.ucsd.forward.query.logical;

/**
 * An interface for indicating which data source will execute the physical counterpart of a logical operator in a physical query
 * plan. All operators in a logical query plan should have an execution data source (DISTRIBUTED NORMAL FORM) in order to generate a
 * distributed physical query plan.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface ExecutionDataSource
{
    
    /**
     * Returns the name of the execution data source.
     * 
     * @return the name of the execution data source.
     */
    public String getExecutionDataSourceName();
    
    /**
     * Sets the name of the execution data source.
     * 
     * @param data_source
     *            the name of the execution data source.
     */
    public void setExecutionDataSourceName(String data_source);
    
}
