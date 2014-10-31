/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.model;

import edu.ucsd.app2you.util.identity.DeepEquality;
import edu.ucsd.app2you.util.identity.EqualUtil;
import edu.ucsd.app2you.util.identity.HashCodeUtil;
import edu.ucsd.app2you.util.identity.Immutable;
import edu.ucsd.app2you.util.logger.Logger;

/**
 * A named parameter in a SQL statement.
 * 
 * @author Kian Win
 * 
 */
public class Parameter implements Immutable, DeepEquality
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(Parameter.class);
    
    private String              m_name;
    
    /**
     * Constructs the parameter.
     * 
     * @param name
     *            - the parameter name.
     */
    public Parameter(String name)
    {
        m_name = name;
    }
    
    /**
     * 
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.app2you.util.identity.DeepEquality#getDeepEqualityObjects()
     */
    @Override
    public Object[] getDeepEqualityObjects()
    {
        return new Object[] { m_name };
    }
    
    /**
     * Returns the parameter name.
     * 
     * @return the parameter name.
     */
    public String getName()
    {
        return m_name;
    }
    
    /**
     * Returns the string representation in SQL.
     * 
     * @return the string representation in SQL.
     */
    public String toSql()
    {
        return ":" + getName();
    }
    
    /**
     * 
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o)
    {
        return EqualUtil.equalsByDeepEquality(this, o);
    }
    
    /**
     * 
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        return HashCodeUtil.hashCodeByDeepEquality(this);
    }
    
    /**
     * 
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return toSql();
    }
    
}
