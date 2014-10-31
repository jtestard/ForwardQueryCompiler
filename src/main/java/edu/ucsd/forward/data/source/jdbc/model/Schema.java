/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.model;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A relational schema.
 * 
 * @author Kian Win
 * @author Yupeng
 */
public class Schema
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(Schema.class);
    
    private String              m_name;
    
    private List<Table>         m_tables;
    
    /**
     * Constructs the schema.
     * 
     * @param name
     *            - the name of the schema.
     */
    public Schema(String name)
    {
        assert (name != null);
        m_name = name;
        m_tables = new ArrayList<Table>();
    }
    
    /**
     * Returns the name.
     * 
     * @return the name.
     */
    public String getName()
    {
        return m_name;
    }
    
    /**
     * Returns the tables in the schema. The list can be updated to add and remove tables from the schema.
     * 
     * @return the tables in the schema.
     */
    public List<Table> getTables()
    {
        return m_tables;
    }
    
}
