/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.model;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A SQL AUTO_INCREMENT constraint.
 * 
 * @author Kian Win
 * 
 */
public class SqlAutoIncrementConstraint implements SqlConstraint
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(SqlAutoIncrementConstraint.class);
    
    private Table               m_table;
    
    private Column              m_column;
    
    /**
     * Constructs the auto increment constraint.
     * 
     * @param table
     *            - the table.
     * @param column
     *            - the column.
     */
    public SqlAutoIncrementConstraint(Table table, Column column)
    {
        assert (table != null);
        assert (column != null);
        
        m_table = table;
        m_column = column;
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
     * Returns the column.
     * 
     * @return the column.
     */
    public Column getColumn()
    {
        return m_column;
    }
}
