/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.model;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.index.IndexDeclaration;
import edu.ucsd.forward.data.index.IndexMethod;

/**
 * An Index in SQL database.
 * 
 * @author Yupeng
 * 
 */
public class Index
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(Index.class);
    
    private List<Column>        m_columns;
    
    private Table               m_table;
    
    private boolean             m_unique;
    
    private IndexMethod         m_method;
    
    private String              m_name;
    
    /**
     * Constructs and Index.
     * 
     * @param name
     *            the name of the index.
     * @param table
     *            the table that the index is built on.
     * @param columns
     *            the columns as the key
     * @param unique
     *            if duplicate records in the index are allowed.
     * @param method
     *            the index method
     */
    public Index(String name, Table table, List<Column> columns, boolean unique, IndexMethod method)
    {
        assert name != null;
        assert table != null;
        assert columns != null;
        assert method != null;
        m_name = name;
        m_table = table;
        m_columns = columns;
        m_unique = unique;
        m_method = method;
    }
    
    /**
     * Gets the name.
     * 
     * @return the name of the index.
     */
    public String getName()
    {
        return m_name;
    }
    
    /**
     * Gets the table.
     * 
     * @return the table.
     */
    public Table getTable()
    {
        return m_table;
    }
    
    /**
     * Gets the columns as the key.
     * 
     * @return the columns as the key.
     */
    public List<Column> getColumns()
    {
        return m_columns;
    }
    
    /**
     * Checks if duplicate records in the index are allowed.
     * 
     * @return <code>true</code> if duplicate records in the index are allowed;<code>false</code> otherwise.
     */
    public boolean isUnique()
    {
        return m_unique;
    }
    
    /**
     * Gets the index method.
     * 
     * @return the index method.
     */
    public IndexMethod getMethod()
    {
        return m_method;
    }
    
    /**
     * Creates a SQL index from an index declaration.
     * 
     * @param declaration
     *            the index declaration.
     * @param table
     *            the existing table.
     * @return the created SQL index.
     */
    public static Index create(IndexDeclaration declaration, Table table)
    {
        List<Column> columns = new ArrayList<Column>();
        for (SchemaPath path : declaration.getKeyPaths())
        {
            columns.add(table.getColumn(path.getLastPathStep()));
        }
        Index index = new Index(declaration.getName(), table, columns, declaration.isUnique(), declaration.getMethod());
        return index;
    }
}
