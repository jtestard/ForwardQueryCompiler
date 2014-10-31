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
 * Statement for the SQL command CREATE SCHEMA.
 * 
 * @author Kian Win
 * 
 */
public class CreateSchemaStatement extends AbstractJdbcStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(CreateSchemaStatement.class);
    
    private SchemaHandle        m_schema_handle;
    
    /**
     * Constructs the statement.
     * 
     * @param schema
     *            - the schema to create.
     */
    public CreateSchemaStatement(Schema schema)
    {
        assert (schema != null);
        m_schema_handle = new SchemaHandle(schema);
    }
    
    /**
     * Constructs the statement.
     * 
     * @param schema_handle
     *            - the schema to create.
     */
    public CreateSchemaStatement(SchemaHandle schema_handle)
    {
        assert (schema_handle != null);
        m_schema_handle = schema_handle;
    }
    
    @Override
    public String toSql(SqlDialect sql_dialect, SqlIdentifierDictionary dictionary)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE SCHEMA ");
        sb.append(m_schema_handle.toSql(dictionary));
        return sb.toString();
    }
}
