/**
 * 
 */
package edu.ucsd.forward.exception;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A line/column range within a file. Holds a start FileLocation and an end FileLocation to indicate a range in a file.
 * 
 * @author Erick Zamora
 */
public class FileRange
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(FileRange.class);
    
    private FileLocation        m_start;
    
    private FileLocation        m_end;
    
    /**
     * Constructor.
     * 
     * @param start
     *            the start file location
     * @param end
     *            the end file location
     */
    public FileRange(FileLocation start, FileLocation end)
    {
        setStart(start);
        setEnd(end);
    }
    
    /**
     * Get the start file location.
     * 
     * @return the start
     */
    public FileLocation getStart()
    {
        return m_start;
    }
    
    /**
     * Set the start file location.
     * 
     * @param start
     *            the start to set
     */
    public void setStart(FileLocation start)
    {
        assert start != null;
        if (m_end != null)
        {
            assert start.getFile() == m_end.getFile() || start.getFile().equals(m_end.getFile());
        }
        m_start = start;
    }
    
    /**
     * Get the end file location.
     * 
     * @return the end
     */
    public FileLocation getEnd()
    {
        return m_end;
    }
    
    /**
     * Set the end file location.
     * 
     * @param end
     *            the end to set
     */
    public void setEnd(FileLocation end)
    {
        assert end != null;
        if (m_start != null)
        {
            assert end.getFile() == m_start.getFile() || end.getFile().equals(m_start.getFile());
        }
        m_end = end;
    }
    
    /**
     * Clears the filename from the underlying file locations.
     */
    public void clearFileName()
    {
        m_start.setFile(null);
        m_end.setFile(null);
    }
    
    /**
     * Returns whether the given file location is in this range.
     * 
     * @param file_location
     *            the file location to check if it exists within this range.
     * @return whether the given file location is in this range.
     */
    public boolean inRange(FileLocation file_location)
    {
        int start_comparison = file_location.compareTo(getStart());
        int end_comparison = file_location.compareTo(getEnd());
        
        return start_comparison >= 0 && end_comparison <= 0;
    }
    
    /**
     * Returns whether the given file range is in this range.
     * 
     * @param file_range
     *            the file range to check if it exists within this range.
     * @return whether the given file range is in this range.
     */
    public boolean inRange(FileRange file_range)
    {
        return inRange(file_range.getStart()) && inRange(file_range.getEnd());
    }
    
    @Override
    public String toString()
    {
        String filename = getStart().getFile() != null ? getStart().getFile() : (getEnd().getFile() == null
                ? getEnd().getFile()
                : "");
        return filename + "[" + getStart().getLine() + ":" + getStart().getColumn() + "-" + getEnd().getLine() + ":"
                + getEnd().getColumn() + "]";
    }
}
