package edu.ucsd.forward.query;

/**
 * Checks whether a SQL++ query construct can be translated into a SQL one.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface SqlCompliance
{
    
    /**
     * Checks whether a SQL++ query construct can be translated into a SQL one.
     * 
     * @return true, when the SQL++ query construct can be translated into a SQL one; otherwise, false.
     */
    public boolean isSqlCompliant();
    
}
