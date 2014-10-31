/**
 * 
 */
package edu.ucsd.forward.exception;

import java.io.Serializable;

/**
 * Represents a location in a multi-line input string. A location is either a column of a line, or starts from a column of a line
 * and ends at another column of the same or a different line. If a column is not specified, then the beginning of the line is
 * assumed, that is, column = 1.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface Location extends Serializable
{
    public static final String UNKNOWN_PATH = "unknown";
    
    /**
     * Gets the resource/file path.
     * 
     * @return resource/file path.
     */
    public String getPath();
    
    /**
     * Sets the path of the location.
     * 
     * @param path
     *            the path of the location.
     */
    public void setPath(String path);
    
    /**
     * Gets the starting line number.
     * 
     * @return the starting line number.
     */
    public int getStartLine();
    
    /**
     * Sets the starting line number.
     * 
     * @param start_line
     *            the starting line number.
     */
    public void setStartLine(int start_line);
    
    /**
     * Gets the starting column number.
     * 
     * @return the starting column number.
     */
    public int getStartColumn();
    
    /**
     * Sets the starting column number.
     * 
     * @param start_column
     *            the starting column number.
     */
    public void setStartColumn(int start_column);
    
    /**
     * Gets the ending line number.
     * 
     * @return the ending line number.
     */
    public int getEndLine();
    
    /**
     * Sets the ending line number.
     * 
     * @param end_line
     *            the ending line number.
     */
    public void setEndLine(int end_line);
    
    /**
     * Gets the ending column number.
     * 
     * @return the ending column number.
     */
    public int getEndColumn();
    
    /**
     * Sets the ending column number.
     * 
     * @param end_column
     *            the ending column number.
     */
    public void setEndColumn(int end_column);
    
    /**
     * Returns a string representation of the location. If there is an ending line and column, then the returned string format is
     * Filename (start_line:start_col-end_line:end_col). Otherwise, it is Filename (start_line:start_col). The filename portion will
     * be omitted if it isn't specified.
     * 
     * @return a string representation of the location.
     */
    public String toString();
    
}
