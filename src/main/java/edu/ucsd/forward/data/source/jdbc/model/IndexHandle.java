/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.model;

import edu.ucsd.app2you.util.identity.DeepEquality;
import edu.ucsd.app2you.util.identity.Immutable;
import edu.ucsd.app2you.util.logger.Logger;

/**
 * A handle to a SQL index.
 * 
 * @author Yupeng
 * 
 */
public class IndexHandle extends AbstractSqlHandle implements Immutable, DeepEquality
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(IndexHandle.class);
    
    private String              m_name;
    
    /**
     * Constructs the index handler.
     * 
     * @param index
     *            the index.
     */
    public IndexHandle(Index index)
    {
        assert index != null;
        m_name = index.getName();
    }
    
    @Override
    public Object[] getDeepEqualityObjects()
    {
        return new Object[] { m_name };
    }
    
    @Override
    public String toSql(SqlIdentifierDictionary dictionary)
    {
        return escape(truncate(m_name, dictionary));
    }
    
    @Override
    public String toSql()
    {
        return escape(m_name);        
    }
}
