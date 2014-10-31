/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.statement;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.jdbc.model.Column;
import edu.ucsd.forward.data.source.jdbc.model.ColumnHandle;
import edu.ucsd.forward.data.source.jdbc.model.Index;
import edu.ucsd.forward.data.source.jdbc.model.IndexHandle;
import edu.ucsd.forward.data.source.jdbc.model.SqlDialect;
import edu.ucsd.forward.data.source.jdbc.model.SqlIdentifierDictionary;
import edu.ucsd.forward.data.source.jdbc.model.TableHandle;

/**
 * Statement for the SQL command CREATE INDEX.
 * 
 * @author Yupeng
 * 
 */
public class CreateIndexStatement extends AbstractJdbcStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(CreateIndexStatement.class);
    
    private Index               m_index;
    
    /**
     * Constructs the statement.
     * 
     * @param index
     *            the index.
     */
    public CreateIndexStatement(Index index)
    {
        assert index != null;
        m_index = index;
    }
    
    @Override
    public String toSql(SqlDialect sql_dialect, SqlIdentifierDictionary dictionary)
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append("CREATE ");
        
        if (m_index.isUnique())
        {
            sb.append("UNIQUE ");
        }
        
        sb.append("INDEX ");
        
        sb.append(m_index.getName() + " ON ");
        
        sb.append(new TableHandle(m_index.getTable()).toSql(dictionary));
        sb.append(" USING ");
        sb.append(m_index.getMethod().name());
        sb.append("(");
        
        boolean first = true;
        for (Column column : m_index.getColumns())
        {
            if (first) first = false;
            else sb.append(", ");
            sb.append(new ColumnHandle(column).toSql(dictionary));
            sb.append(" ");
        }
        
        sb.append(")");
        
        return sb.toString();
    }
}
