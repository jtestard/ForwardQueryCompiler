/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.model;

import java.util.ArrayList;
import java.util.regex.Pattern;

import edu.ucsd.app2you.util.identity.AbstractHandle;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.io.IoUtil;

/**
 * Abstract class for implementing SQL handles.
 * 
 * @author Kian Win
 * @author Vicky Papavasileiou
 */
public abstract class AbstractSqlHandle extends AbstractHandle implements SqlHandle
{
    @SuppressWarnings("unused")
    private static final Logger  log                = Logger.getLogger(AbstractSqlHandle.class);
    
    // Be slightly stricter than SQL - allow only alphanumeric characters (and underscore)
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");
    
    private static ArrayList<String> keywords = new ArrayList<String>();
    
    static
    {
        String str = IoUtil.getResourceAsString(AbstractSqlHandle.class, "postgresql_keywords.txt");
        
        for(String s: str.split("\n"))
        {
            keywords.add(s);
        }
        
    }
    
    
    /**
     * Escapes a string according to SQL identifier rules.
     * 
     * @param s
     *            - the identifier string.
     * @return the escaped identifier string.
     */
    public static String escape(String s)
    {
        if (IDENTIFIER_PATTERN.matcher(s).matches() && !keywords.contains(s.toUpperCase()))
        {
            // Use the original name if it is a valid identifier
            return s;
        }
        else
        {
            // ... otherwise escaping is needed
            StringBuilder sb = new StringBuilder();
            
            // Double quote the identifier
            sb.append("\"");
            
            // A double quote is doubly escaped
            sb.append(s.replace("\"", "\"\""));
            
            sb.append("\"");
            
            return sb.toString();
        }
    }
    
    /**
     * Returns the truncated unambiguous system string for a given pretty string.
     * 
     * @param s
     *            - the pretty string.
     * @param dictionary
     *            the dictionary that stores the correspondence between an original (pretty) SQL identifier and a shorter version
     * @return the truncated unambiguous system string.
     */
    public static String truncate(String s, SqlIdentifierDictionary dictionary)
    {
        return dictionary.getTruncatedSystemString(s);
    }
    
    /**
     * 
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.app2you.util.identity.AbstractHandle#toString()
     */
    @Override
    public String toString()
    {
        return toSql();
    }
}
