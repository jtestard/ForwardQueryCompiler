/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.statement;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.jdbc.model.Index;
import edu.ucsd.forward.data.source.jdbc.model.IndexHandle;
import edu.ucsd.forward.data.source.jdbc.model.SqlDialect;
import edu.ucsd.forward.data.source.jdbc.model.SqlIdentifierDictionary;

/**
 * Statement for the SQL command DROP INDEX.
 * 
 * @author Yupeng
 * 
 */
public class DropIndexStatement extends AbstractJdbcStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DropIndexStatement.class);
    
    private boolean             m_strict;
    
    private Index               m_index;
    
    /**
     * Constructs the statement.
     * 
     * @param index
     *            the index to drop.
     */
    public DropIndexStatement(Index index)
    {
        assert index != null;
        m_index = index;
    }
    
    /**
     * Returns whether the statement fails when there is no existing index to drop.
     * 
     * @return <code>true</code> if the statement fails when there is no existing index to drop; <code>false</code> otherwise.
     */
    public boolean getStrict()
    {
        return m_strict;
    }
    
    /**
     * Sets whether the statement fails when there is no existing index to drop.
     * 
     * @param strict
     *            - whether the statement fails when there is no existing index to drop.
     */
    public void setStrict(boolean strict)
    {
        m_strict = strict;
    }
    
    @Override
    public String toSql(SqlDialect sql_dialect, SqlIdentifierDictionary dictionary)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP INDEX ");
        if (!m_strict) sb.append("IF EXISTS ");
        IndexHandle handle = new IndexHandle(m_index);
        sb.append(handle.toSql(dictionary));
        sb.append(" CASCADE");
        return sb.toString();
    }
}
