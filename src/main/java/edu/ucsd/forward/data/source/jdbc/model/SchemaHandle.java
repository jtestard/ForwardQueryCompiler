/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.model;

import edu.ucsd.app2you.util.identity.DeepEquality;
import edu.ucsd.app2you.util.identity.Immutable;
import edu.ucsd.app2you.util.logger.Logger;

/**
 * A handle to a relational schema.
 * 
 * @author Kian Win
 * 
 */
public class SchemaHandle extends AbstractSqlHandle implements Immutable, DeepEquality
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(SchemaHandle.class);
    
    private String              m_name;
    
    /**
     * Constructs the handle.
     * 
     * @param name
     *            - the name of the schema.
     */
    public SchemaHandle(String name)
    {
        assert (name != null);
        m_name = name;
    }
    
    /**
     * Constructs the handle.
     * 
     * @param schema
     *            - the schema.
     */
    public SchemaHandle(Schema schema)
    {
        m_name = schema.getName();
    }
    
    @Override
    public String toSql()
    {
        return escape(m_name);
    }
    
    @Override
    public Object[] getDeepEqualityObjects()
    {
        return new Object[] { m_name };
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
    
    @Override
    public String toSql(SqlIdentifierDictionary dictionary)
    {
        return escape(truncate(m_name, dictionary));
    }
    
}
