/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.statement;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.jdbc.model.Schema;
import edu.ucsd.forward.data.source.jdbc.model.SchemaHandle;
import edu.ucsd.forward.data.source.jdbc.model.SqlDialect;
import edu.ucsd.forward.data.source.jdbc.model.SqlIdentifierDictionary;

/**
 * Statement for the SQL command DROP SCHEMA.
 * 
 * @author Kian Win
 * 
 */
public class DropSchemaStatement extends AbstractJdbcStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DropSchemaStatement.class);
    
    private SchemaHandle        m_schema_handle;
    
    private boolean             m_strict;
    
    /**
     * Constructs the statement.
     * 
     * @param schema
     *            - the schema to drop.
     */
    public DropSchemaStatement(Schema schema)
    {
        assert (schema != null);
        m_schema_handle = new SchemaHandle(schema);
    }
    
    /**
     * Constructs the statement.
     * 
     * @param schema
     *            - the schema to drop.
     */
    public DropSchemaStatement(SchemaHandle schema)
    {
        assert (schema != null);
        m_schema_handle = schema;
    }
    
    /**
     * Returns whether the statement fails when there is no existing schema to drop.
     * 
     * @return <code>true</code> if the statement fails when there is no existing schema to drop; <code>false</code> otherwise.
     */
    public boolean getStrict()
    {
        return m_strict;
    }
    
    /**
     * Sets whether the statement fails when there is no existing schema to drop.
     * 
     * @param strict
     *            - whether the statement fails when there is no existing schema to drop.
     */
    public void setStrict(boolean strict)
    {
        m_strict = strict;
    }
    
    @Override
    public String toSql(SqlDialect sql_dialect, SqlIdentifierDictionary dictionary)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP SCHEMA ");
        if (!m_strict) sb.append("IF EXISTS ");
        sb.append(m_schema_handle.toSql(dictionary));
        
        if (sql_dialect == SqlDialect.POSTGRESQL)
        {
            sb.append(" CASCADE");
        }
        else
        {
            assert (sql_dialect == SqlDialect.H2);
            // Nothing to do - H2 automatically cascades
        }
        
        return sb.toString();
    }
}
