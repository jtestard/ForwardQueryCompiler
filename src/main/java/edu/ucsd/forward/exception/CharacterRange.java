/**
 * 
 */
package edu.ucsd.forward.exception;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A character range in a string. Holds start and end index (inclusive) and should be
 * zero-indexed.
 * 
 * @author Erick Zamora
 */
public class CharacterRange implements Comparable<CharacterRange>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(CharacterRange.class);
    
    private int                 m_start;
    
    private int                 m_end;
    
    /**
     * Constructor.
     * 
     * @param start
     *            the start file location
     * @param end
     *            the end file location
     */
    public CharacterRange(int start, int end)
    {
        m_start = start;
        m_end = end;
    }
    
    /**
     * Get the start of the character range.
     * 
     * @return the start
     */
    public int getStart()
    {
        return m_start;
    }
    
    /**
     * Get the end of the character range.
     * 
     * @return the end
     */
    public int getEnd()
    {
        return m_end;
    }
    
    /**
     * Returns whether the given character range is in this range.
     * 
     * @param character_range
     *            the character range to check if it exists within this range.
     * @return whether the given character range is in this range.
     */
    public boolean inRange(CharacterRange character_range)
    {
        int start_compare = ((Integer) character_range.getStart()).compareTo(getStart());
        int end_compare = ((Integer) character_range.getEnd()).compareTo(getEnd());
        return start_compare >= 0 && end_compare <= 0;
    }
    
    @Override
    public String toString()
    {
        return String.format("{%d,%d}", getStart(), getEnd());
    }
    
    /**
     * Compares two character ranges only by their start position.
     * 
     * @param char_range
     *            the character range to compare against
     * @return less than 0 if less, 0 if equal, greater than 0 if greater.
     */
    @Override
    public int compareTo(CharacterRange char_range)
    {
        return ((Integer) m_start).compareTo(char_range.getStart());
    }
}
