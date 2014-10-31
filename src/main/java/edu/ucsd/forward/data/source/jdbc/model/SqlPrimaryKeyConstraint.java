/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A SQL PRIMARY KEY constraint.
 * 
 * @author Kian Win
 * 
 */
public class SqlPrimaryKeyConstraint implements SqlConstraint
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(SqlPrimaryKeyConstraint.class);
    
    private Table               m_table;
    
    private List<Column>        m_columns;
    
    /**
     * Constructs the primary key constraint.
     * 
     * @param table
     *            - the table.
     * @param columns
     *            - the columns.
     */
    public SqlPrimaryKeyConstraint(Table table, List<Column> columns)
    {
        assert (table != null);
        assert (columns != null);
        assert (!columns.isEmpty());
        
        m_table = table;
        m_columns = new ArrayList<Column>(columns);
    }
    
    /**
     * Returns the table.
     * 
     * @return the table.
     */
    public Table getTable()
    {
        return m_table;
    }
    
    /**
     * Returns the columns.
     * 
     * @return the columns.
     */
    public List<Column> getColumns()
    {
        return Collections.unmodifiableList(m_columns);
    }
}
