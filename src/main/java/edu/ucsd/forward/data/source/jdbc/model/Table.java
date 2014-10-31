/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.model;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.TupleType.AttributeEntry;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.ast.literal.StringLiteral;

/**
 * A table in a SQL database.
 * 
 * @author Kian Win
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public class Table
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(Table.class);
    
    /**
     * An enumeration of the table scopes as defined in the SQL standard. In case of PERSISTENT, the table is present in a JDBC data
     * source for the lifetime of the data source or if it is explicitly dropped. In case of LOCAL_TEMPORARY, the table is present
     * in a JSBC data source for the lifetime of transaction or if it is explicitly dropped.
     */
    public enum TableScope
    {
        PERSISTENT, LOCAL_TEMPORARY;
    }
    
    /**
     * The scope of the table.
     */
    private TableScope   m_scope;
    
    private Schema       m_schema;
    
    private String       m_name;
    
    private List<Column> m_columns;
    
    /**
     * Constructs the table.
     * 
     * @param scope
     *            - the scope of the table.
     * @param schema
     *            - the parent schema.
     * @param name
     *            - the name of the table.
     */
    public Table(TableScope scope, Schema schema, String name)
    {
        assert (scope != null);
        m_scope = scope;
        
        assert (schema != null);
        m_schema = schema;
        
        assert (name != null);
        m_name = name;
        
        m_columns = new ArrayList<Column>();
    }
    
    /**
     * Returns the scope of the table.
     * 
     * @return the scope of the table.
     */
    public TableScope getScope()
    {
        return m_scope;
    }
    
    /**
     * Returns the parent schema.
     * 
     * @return the parent schema.
     */
    public Schema getSchema()
    {
        return m_schema;
    }
    
    /**
     * Returns the name of the table.
     * 
     * @return the name of the table.
     */
    public String getName()
    {
        return m_name;
    }
    
    /**
     * Returns the columns in the table. The list can be updated to add and remove columns from the table.
     * 
     * @return the tables in the table.
     */
    public List<Column> getColumns()
    {
        return m_columns;
    }
    
    /**
     * Returns the column of the given name.
     * 
     * @param name
     *            the column name.
     * @return the column if it exists; <code>null</code> otherwise.
     */
    public Column getColumn(String name)
    {
        assert (name != null);
        for (Column column : m_columns)
        {
            if (column.getName().equals(name)) return column;
        }
        return null;
    }
    
    /**
     * Creates a table for a particular collection type.
     * 
     * @param scope
     *            the table scope
     * @param schema
     *            the schema
     * @param table_name
     *            the name of the table
     * @param collection_type
     *            the SQL-compliant collection type
     * @return the table for a particular collection type.
     */
    public static Table create(TableScope scope, String schema, String table_name, CollectionType collection_type)
    {
        Table table = new Table(scope, new Schema(schema), table_name);
        
        TupleType tuple_type = collection_type.getTupleType();
        assert (tuple_type.getSize() != 0);
        
        // Build table
        for (AttributeEntry entry : tuple_type)
        {
            // The column name is the attribute name
            String column_name = entry.getName();
            
            // The column type can be looked up through the type converter
            ScalarType scalar_type = (ScalarType) entry.getType();
            String sql_type_name = SqlTypeConverter.getSqlTypeName(scalar_type.getClass());
            
            Value default_value = scalar_type.getDefaultValue();
            String default_str = (default_value instanceof StringValue)
                    ? StringLiteral.escape((StringValue) default_value)
                    : default_value.toString();
            
            Column column = new Column(table, column_name, sql_type_name, default_str);
            
            table.getColumns().add(column);
        }
        
        return table;
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(m_name);
        sb.append(" ( \n");
        
        for (Column column : getColumns())
        {
            sb.append(column);
            sb.append(",\n");
        }
        
        sb.append(")");
        
        return sb.toString();
    }
}
