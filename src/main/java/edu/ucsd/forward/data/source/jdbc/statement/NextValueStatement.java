/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.statement;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.jdbc.model.SequenceHandle;
import edu.ucsd.forward.data.source.jdbc.model.SqlDialect;
import edu.ucsd.forward.data.source.jdbc.model.SqlIdentifierDictionary;

/**
 * Statement for the SQL command SELECT NEXT VALUE FOR.
 * 
 * @author Kian Win
 * 
 */
public class NextValueStatement extends AbstractJdbcStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(NextValueStatement.class);
    
    private SequenceHandle      m_sequence_handle;
    
    /**
     * Constructs the statement.
     * 
     * @param sequence_handle
     *            - the sequence to get the next value for.
     */
    public NextValueStatement(SequenceHandle sequence_handle)
    {
        assert (sequence_handle != null);
        m_sequence_handle = sequence_handle;
    }
    
    @Override
    public String toSql(SqlDialect sql_dialect, SqlIdentifierDictionary dictionary)
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append("SELECT CAST(nextval('");
        sb.append(m_sequence_handle.toSql(dictionary));
        sb.append("') AS integer)");
        
        return sb.toString();
    }
}
