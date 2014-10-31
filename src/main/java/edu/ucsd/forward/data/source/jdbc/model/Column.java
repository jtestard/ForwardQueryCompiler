/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.model;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A column in a SQL table.
 * 
 * @author Kian Win
 * @author Yupeng
 */
public class Column
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(Column.class);
    
    private Table               m_table;
    
    private String              m_name;
    
    private String              m_sql_type_name;
    
    private String              m_default_value;
    
    /**
     * Constructs the column.
     * 
     * @param table
     *            - the parent table.
     * @param name
     *            - the name of the column.
     * @param sql_type_name
     *            - the SQL type name of the column.
     */
    public Column(Table table, String name, String sql_type_name)
    {
        this(table, name, sql_type_name, null);
    }
    
    /**
     * Constructs the column.
     * 
     * @param table
     *            - the parent table.
     * @param name
     *            - the name of the column.
     * @param sql_type_name
     *            - the SQL type name of the column.
     * @param default_value
     *            the default value
     */
    public Column(Table table, String name, String sql_type_name, String default_value)
    {
        assert (table != null);
        assert (name != null);
        assert (sql_type_name != null);
        
        m_table = table;
        m_name = name;
        m_sql_type_name = sql_type_name;
        m_default_value = default_value;
    }
    
    /**
     * Returns the parent table.
     * 
     * @return the parent table.
     */
    public Table getTable()
    {
        return m_table;
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
     * Returns the SQL type name.
     * 
     * @return the SQL type name.
     */
    public String getSqlTypeName()
    {
        return m_sql_type_name;
    }
    
    /**
     * Checks if the column has default value.
     * 
     * @return <code>true</code> if the column has default value, <code>false</code> otherwise.
     */
    public boolean hasDefaultValue()
    {
        return m_default_value != null && !m_default_value.equals("");
    }
    
    /**
     * Gets the default value.
     * 
     * @return the default value.
     */
    public String getDefaultValue()
    {
        return m_default_value;
    }
    
    @Override
    public String toString()
    {
        String s = m_name + " " + m_sql_type_name;
        if (hasDefaultValue()) s += " " + m_default_value;
        return s;
    }
}
