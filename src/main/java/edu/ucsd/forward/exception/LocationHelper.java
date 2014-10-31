/**
 * 
 */
package edu.ucsd.forward.exception;

/**
 * Utility methods for location.
 * 
 * @author Hongping Lim
 * 
 */
public final class LocationHelper
{
    /**
     * Private constructor.
     */
    private LocationHelper()
    {
        
    }
    
    /**
     * Offsets the query location using the element location information, returning the derived location.
     * 
     * @param element_location
     *            element location
     * @param query_location
     *            query location
     * @return location
     */
    public static Location offset(Location element_location, Location query_location)
    {
        int start_line = query_location.getStartLine();
        int start_column = query_location.getStartColumn();
        if (start_line == 1)
        {
            start_column = start_column + element_location.getStartColumn() - 1;
        }
        start_line = start_line + element_location.getStartLine() - 1;
        
        int end_line = query_location.getEndLine();
        int end_column = query_location.getEndColumn();
        if (end_line == 1)
        {
            end_column = end_column + element_location.getStartColumn() - 1;
        }
        end_line = end_line + element_location.getStartLine() - 1;
        
        LocationImpl loc = new LocationImpl(element_location.getPath(), start_line, start_column, end_line, end_column);
        return loc;
    }
}
