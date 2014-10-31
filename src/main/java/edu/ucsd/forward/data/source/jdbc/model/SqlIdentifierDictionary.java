/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.model;

import java.util.Comparator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.collections15.SortedBidiMap;
import org.apache.commons.collections15.bidimap.DualTreeBidiMap;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A dictionary to store the correspondence between an original (pretty) SQL identifier, and a shorter version that includes a
 * system-generated id to make the identifier unambiguous when truncated.
 * 
 * @author Kian Win
 * 
 */
public class SqlIdentifierDictionary
{
    @SuppressWarnings("unused")
    private static final Logger           log                   = Logger.getLogger(SqlIdentifierDictionary.class);
    
    private static final int              MAX_IDENTIFIER_LENGTH = 63;
    
    private static final int              PADDING               = 4;
    
    private SortedBidiMap<String, String> m_pretty_to_long_system;
    
    private int                           m_counter;
    
    /**
     * Constructs the dictionary.
     */
    public SqlIdentifierDictionary()
    {
        m_pretty_to_long_system = new DualTreeBidiMap<String, String>(new DescLengthComparator(), new TruncateComparator());
        m_counter = 1;
    }
    
    /**
     * Returns the pretty string for a given system string. The system string will first be truncated to the maximum identifier
     * limit.
     * 
     * @param system_string
     *            - the system string.
     * @return the pretty string.
     */
    public String getPrettyString(String system_string)
    {
        if (system_string.length() < MAX_IDENTIFIER_LENGTH) return null;
        // No need to truncate here, as the TruncateComparator already does this
        return m_pretty_to_long_system.getKey(system_string);
    }
    
    /**
     * Returns the truncated unambiguous system string for a given pretty string.
     * 
     * @param pretty_string
     *            - the pretty string.
     * @return the truncated unambiguous system string.
     */
    public String getTruncatedSystemString(String pretty_string)
    {
        assert (pretty_string != null);
        if (pretty_string.length() <= MAX_IDENTIFIER_LENGTH) return pretty_string;
        
        String long_system_string = getLongSystemString(pretty_string);
        return long_system_string.substring(0, MAX_IDENTIFIER_LENGTH);
    }
    
    /**
     * Returns the long unambiguous system string for a given pretty string. The long unambiguous system string is guaranteed to
     * have the pretty string as a suffix (so that a human can still understand it).
     * 
     * @param pretty_string
     *            - the pretty string.
     * @return the long unambiguous system string.
     */
    public String getLongSystemString(String pretty_string)
    {
        assert (pretty_string != null);
        if (pretty_string.length() <= MAX_IDENTIFIER_LENGTH) return pretty_string;
        
        String long_system_string = m_pretty_to_long_system.get(pretty_string);
        if (long_system_string != null) return long_system_string;
        
        String prefix = String.format("x%0" + PADDING + "d_", m_counter++);
        long_system_string = prefix + pretty_string;
        m_pretty_to_long_system.put(pretty_string, long_system_string);
        
        return long_system_string;
    }
    
    /**
     * Replaces all occurrences of all pretty strings in the given string. The replacement is guaranteed to be performed in
     * descending order of the pretty strings' length.
     * 
     * @param s
     *            - the string.
     * @return the replaced string.
     */
    public String replaceAllPrettyStrings(String s)
    {
        // The DescLengthComparator ensures iteration in the correct order
        // FIXME: Test this in TestQuery.testLongName
        String cur = s;
        for (Map.Entry<String, String> e : m_pretty_to_long_system.entrySet())
        {
            String long_string = e.getKey();
            String unique_string = e.getValue();
            
            // Use Pattern.quote so that the regex is treated as a string literal
            cur = cur.replaceAll(Pattern.quote(long_string), unique_string);
        }
        return cur;
    }
    
    /**
     * Comparator that sorts strings in descending order of their lengths.
     * 
     * @author Kian Win
     * 
     */
    private static class DescLengthComparator implements Comparator<String>
    {
        /**
         * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
         * <br>
         * <b>See original method below.</b> <br>
         * 
         * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
         */
        @Override
        public int compare(String s1, String s2)
        {
            int l1 = s1.length();
            int l2 = s2.length();
            
            if (l1 < l2) return 1;
            if (l1 > l2) return -1;
            return s1.compareTo(s2);
        }
    }
    
    /**
     * Comparator that sorts strings by their truncated versions.
     * 
     * @author Kian Win
     * 
     */
    private static class TruncateComparator implements Comparator<String>
    {
        /**
         * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
         * <br>
         * <b>See original method below.</b> <br>
         * 
         * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
         */
        @Override
        public int compare(String s1, String s2)
        {
            return s1.substring(0, MAX_IDENTIFIER_LENGTH).compareTo(s2.substring(0, MAX_IDENTIFIER_LENGTH));
        }
        
    }
}
