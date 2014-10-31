/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.model;

import edu.ucsd.app2you.util.identity.Handle;

/**
 * A handle to a SQL data structure.
 * 
 * @author Kian Win
 * 
 */
public interface SqlHandle extends Handle
{
    /**
     * Returns the SQL name of the data structure. Special characters in the name are escaped, so it can be used directly in SQL
     * commands. A dictionary is given to store the correspondence between an original (pretty) SQL identifier, and a shorter
     * version that includes a system-generated id to make the identifier unambiguous when truncated.
     * 
     * @param dictionary
     *            the dictionary stores the correspondence between an original (pretty) SQL identifier, and a shorter version
     * 
     * @return the SQL name of the data structure.
     */
    public String toSql(SqlIdentifierDictionary dictionary);
    
    /**
     * Returns the SQL name of the data structure. Special characters in the name are escaped, so it can be used directly in SQL
     * commands.
     * 
     * @return the SQL name of the data structure.
     */
    public String toSql();
}
