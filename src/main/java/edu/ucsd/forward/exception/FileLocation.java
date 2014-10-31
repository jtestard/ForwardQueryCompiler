/**
 * 
 */
package edu.ucsd.forward.exception;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A location/position within a file. Holds line and column positions and a file name.
 * 
 * @author Erick Zamora
 */
public class FileLocation implements Comparable<FileLocation>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(FileLocation.class);
    
    private int                 m_column;
    
    private int                 m_line;
    
    private String              m_file;
    
    /**
     * Constructor.
     * 
     * @param file
     *            the file
     * @param line
     *            the line
     * @param column
     *            the column
     */
    public FileLocation(String file, int line, int column)
    {
        setFile(file);
        setLine(line);
        setColumn(column);
    }
    
    /**
     * Constructor.
     * 
     * @param file_location
     *            the file location to copy.
     */
    public FileLocation(FileLocation file_location)
    {
        setFile(file_location.getFile());
        setLine(file_location.getLine());
        setColumn(file_location.getColumn());
    }
    
    /**
     * Gets the column.
     * 
     * @return the column
     */
    public int getColumn()
    {
        return m_column;
    }
    
    /**
     * Sets the column.
     * 
     * @param column
     *            the column to set
     */
    public void setColumn(int column)
    {
        m_column = column;
    }
    
    /**
     * Gets the line.
     * 
     * @return the line
     */
    public int getLine()
    {
        return m_line;
    }
    
    /**
     * Sets the line.
     * 
     * @param line
     *            the line to set
     */
    public void setLine(int line)
    {
        m_line = line;
    }
    
    /**
     * Gets the file.
     * 
     * @return the file
     */
    public String getFile()
    {
        return m_file;
    }
    
    /**
     * Sets the file.
     * 
     * @param file
     *            the file to set
     */
    public void setFile(String file)
    {
        m_file = file;
    }
    
    @Override
    public int compareTo(FileLocation file_location)
    {
        int file_compare;
        if (getFile() != null && file_location.getFile() != null)
        {
            file_compare = getFile().compareTo(file_location.getFile());
        }
        else
        {
            file_compare = 0;
        }
        int line_compare = ((Integer) getLine()).compareTo(file_location.getLine());
        int column_compare = ((Integer) getColumn()).compareTo(file_location.getColumn());
        
        // If files are not the same, return the file name comparison
        if (file_compare != 0)
        {
            return file_compare;
        }
        else if (line_compare != 0)
        {
            // If lines are not the same, return the line comparison
            return line_compare;
        }
        else
        {
            // If file and lines are the same, return the column comparison
            return column_compare;
        }
    }
    
    @Override
    public String toString()
    {
        return (getFile() == null ? "null" : getFile()) + "[" + getLine() + "," + getColumn() + "]";
    }
}
