/**
 * 
 */
package edu.ucsd.forward.exception;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * Pattern matcher class that given a regex pattern and an input, will iterate over the matches and return a match object that
 * contains line/column/index positions for the start and end of the match as well as provide access to the groups in the regex
 * pattern.
 * 
 * @author Erick Zamora
 */
public class LineMatcher
{
    @SuppressWarnings("unused")
    private static final Logger          log = Logger.getLogger(LineMatcher.class);
    
    private String                       m_input;
    
    private Pattern                      m_pattern;
    
    private Matcher                      m_matcher;
    
    private Map<CharacterRange, Integer> m_char_ranges_to_lines;
    
    private Match                        m_last_match;
    
    private Match                        m_current_match;
    
    private Boolean                      m_is_first_match;
    
    /**
     * Public constructor.
     * 
     * @param regex
     *            the regex pattern used for matching.
     * @param input
     *            the input string to match on.
     */
    public LineMatcher(String regex, String input)
    {
        assert (regex != null);
        assert (input != null);
        m_pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        m_matcher = m_pattern.matcher(input);
        m_input = input;
        setUpCharRangesToLines();
    }
    
    /*
     * =============================================================================================================================
     * Public methods
     * =============================================================================================================================
     */
    
    /**
     * Attempts to find the next subsequence of the input sequence that matches the pattern. This method starts at the beginning of
     * this matcher's region, or, if a previous invocation of the method was successful and the matcher has not since been reset, at
     * the first character not matched by the previous match.
     * 
     * If the match succeeds then more information can be obtained via the getMatch() which returns the matched object.
     * 
     * @return whether another match exists.
     */
    public boolean next()
    {
        m_last_match = m_current_match;
        
        if (m_matcher.find())
        {
            Match match = new Match();
            match.setStartIndex(m_matcher.start());
            match.setEndIndex(m_matcher.end() - 1);
            match.setStartLocation(getLocation(m_matcher.start()));
            match.setEndLocation(getLocation(m_matcher.end() - 1));
            match.setGroups(m_matcher);
            m_current_match = match;
            m_is_first_match = m_is_first_match == null ? true : false;
            return true;
        }
        else
        {
            m_last_match = m_current_match;
            m_current_match = null;
        }
        return false;
    }
    
    /**
     * Returns the most recently matched object.
     * 
     * @return the match.
     */
    public Match getMatch()
    {
        return m_current_match;
    }
    
    /**
     * Returns the previously matched object.
     * 
     * @return the previous match.
     */
    public Match getLastMatch()
    {
        return m_last_match;
    }
    
    /**
     * Returns whether the current match is the first match. If no matches have been found, returns null.
     * 
     * @return whether the current match is the first match, null if no matches have been found.
     */
    public Boolean isFirstMatch()
    {
        return m_is_first_match;
    }
    
    /**
     * Given an index, this returns a FileLocation object that holds the start line and start column.
     * 
     * @param index
     *            the position in the string to convert to a location.
     * @return the location.
     */
    public FileLocation getLocation(int index)
    {
        CharacterRange range = new CharacterRange(index, index);
        for (Entry<CharacterRange, Integer> entry : m_char_ranges_to_lines.entrySet())
        {
            if (entry.getKey().inRange(range))
            {
                // The character ranges are zero-indexed, and FileLocation columns start at index 1, we add 1
                return new FileLocation(null, entry.getValue(), range.getStart() - entry.getKey().getStart() + 1);
            }
        }
        return null;
    }
    
    /**
     * Given a FileLocation, this returns an index.
     * 
     * @param location
     *            the location in the string to convert to an index.
     * @return the index position.
     */
    public int getIndex(FileLocation location)
    {
        int start_line = location.getLine();
        for (Entry<CharacterRange, Integer> entry : m_char_ranges_to_lines.entrySet())
        {
            if (entry.getValue() != start_line) continue;
            
            // The character ranges are zero-indexed, and FileLocation columns start at index 1, we subtract 1
            return entry.getKey().getStart() + location.getColumn() - 1;
        }
        return -1;
    }
    
    /**
     * Get start file location of group or match.
     * 
     * @param group
     *            if provided, the start of the group otherwise the start of the match.
     * @return the start file location of the group
     */
    public FileLocation start(Integer group)
    {
        int index = group == null ? m_matcher.start() : m_matcher.start(group);
        return getLocation(index);
    }
    
    /**
     * Get end file location of group or match.
     * 
     * @param group
     *            if provided, the start of the group otherwise the end of the match.
     * @return the end file location of the group or match.
     */
    public FileLocation end(Integer group)
    {
        int index = group == null ? m_matcher.end() : m_matcher.end(group);
        return getLocation(index);
    }
    
    /*
     * =============================================================================================================================
     * Private methods
     * =============================================================================================================================
     */
    
    /**
     * Sets up the mapping of character ranges to line.
     */
    private void setUpCharRangesToLines()
    {
        m_char_ranges_to_lines = new LinkedHashMap<CharacterRange, Integer>();
        String[] lines = m_input.split("\n", -1);
        
        int char_count = 0;
        int line_counter = 1;
        for (String line : lines)
        {
            if (line.isEmpty() && char_count == m_input.length())
            {
                break;
            }
            int start = char_count;
            // We add 1 to account for new line
            int end = char_count + line.length();
            m_char_ranges_to_lines.put(new CharacterRange(start, end), line_counter++);
            char_count = end + 1;
        }
    }
    
    /*
     * =============================================================================================================================
     * Public inner class.
     * =============================================================================================================================
     */
    
    /**
     * Match object.
     * 
     * @author Erick Zamora
     */
    public class Match
    {
        private FileLocation m_start_location;
        
        private FileLocation m_end_location;
        
        private int          m_start_index;
        
        private int          m_end_index;
        
        private String[]     m_groups;
        
        /**
         * Public constructor.
         */
        public Match()
        {
        }
        
        /*
         * ========================================================================================================================
         * Public getters
         * ========================================================================================================================
         */
        
        /**
         * Gets the start location.
         * 
         * @return the start location
         */
        public FileLocation getStartLocation()
        {
            return m_start_location;
        }
        
        /**
         * Gets the end location.
         * 
         * @return the end location
         */
        public FileLocation getEndLocation()
        {
            return m_end_location;
        }
        
        /**
         * Gets the start index.
         * 
         * @return the start index
         */
        public int getStartIndex()
        {
            return m_start_index;
        }
        
        /**
         * Gets the end index.
         * 
         * @return the end index
         */
        public int getEndIndex()
        {
            return m_end_index;
        }
        
        /**
         * Returns the input subsequence captured by the given group during the previous match operation.
         * 
         * Capturing groups are indexed from left to right, starting at one. Group zero denotes the entire pattern, so the
         * expression m.group(0) is equivalent to m.group().
         * 
         * If the match was successful but the group specified failed to match any part of the input sequence, then null is
         * returned. Note that some groups, for example (a*), match the empty string. This method will return the empty string when
         * such a group successfully matches the empty string in the input.
         * 
         * @param index
         *            the index
         * @return group at given index.
         */
        public String group(int index)
        {
            return m_groups[index];
        }
        
        /**
         * Returns the input subsequence matched by the current match.
         * 
         * Note that some patterns, for example a*, match the empty string. This method will return the empty string when the
         * pattern successfully matches the empty string in the input.
         * 
         * @return The (possibly empty) subsequence matched by the previous match, in string
         */
        public String group()
        {
            return m_groups[0];
        }
        
        /*
         * ========================================================================================================================
         * Private setters
         * ========================================================================================================================
         */
        
        /**
         * Sets the start location.
         * 
         * @param start_location
         *            the start location to set
         */
        private void setStartLocation(FileLocation start_location)
        {
            m_start_location = start_location;
        }
        
        /**
         * Sets the end location.
         * 
         * @param end_location
         *            the end location to set
         */
        private void setEndLocation(FileLocation end_location)
        {
            m_end_location = end_location;
        }
        
        /**
         * Sets the start index.
         * 
         * @param start_index
         *            the start index to set
         */
        private void setStartIndex(int start_index)
        {
            m_start_index = start_index;
        }
        
        /**
         * Sets the end index.
         * 
         * @param end_index
         *            the end index to set
         */
        private void setEndIndex(int end_index)
        {
            m_end_index = end_index;
        }
        
        /**
         * Sets the groups.
         * 
         * @param matcher
         *            the matcher object that hold group count information and groups.
         */
        private void setGroups(Matcher matcher)
        {
            m_groups = new String[matcher.groupCount() + 1];
            for (int i = 0; i <= matcher.groupCount(); i++)
            {
                m_groups[i] = matcher.group(i);
            }
        }
    }
}
