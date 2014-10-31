/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.statement;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.jdbc.model.SchemaHandle;
import edu.ucsd.forward.data.source.jdbc.model.SqlDialect;
import edu.ucsd.forward.data.source.jdbc.model.SqlIdentifierDictionary;

/**
 * Statement for the SQL command SET search_path.
 * 
 * @author Kian Win
 * @author Yupeng
 */
public class SetSearchPathStatement extends AbstractJdbcStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(SetSearchPathStatement.class);
    
    private List<SchemaHandle>  m_schema_handles;
    
    /**
     * Constructs the statement.
     * 
     * @param schema_handles
     *            - the schemas on the search path.
     */
    public SetSearchPathStatement(List<SchemaHandle> schema_handles)
    {
        assert (schema_handles != null);
        m_schema_handles = schema_handles;
    }
    
    @Override
    public String toSql(SqlDialect sql_dialect, SqlIdentifierDictionary dictionary)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SET search_path TO ");
        
        boolean first = true;
        for (SchemaHandle schema : m_schema_handles)
        {
            if (!first) sb.append(", ");
            first = false;
            
            sb.append(schema.getName());
        }
        
        return sb.toString();
    }
}
