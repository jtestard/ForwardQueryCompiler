/**
 * 
 */
package edu.ucsd.forward.query.logical.term;

import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.util.NameGenerator;

/**
 * Abstract implementation of the variable interface.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public abstract class AbstractVariable extends AbstractTerm implements Variable
{
    @SuppressWarnings("unused")
    private static final Logger log             = Logger.getLogger(AbstractVariable.class);
    
    /**
     * The name of the variable that is unique within a logical plan.
     */
    private String              m_name;
    
    /**
     * The default alias of the variable that might not be unique within a logical plan. This is used when SELECT * is encountered.
     */
    private String              m_default_alias;
    
    /**
     * The index of the variable in an input binding.
     */
    private int                 m_binding_index = -1;
    
    /**
     * Constructor.
     * 
     * @param name
     *            the name of the variable.
     * @param default_alias
     *            the default alias of the variable.
     */
    public AbstractVariable(String name, String default_alias)
    {
        assert (name != null);
        assert (default_alias != null);
        
        m_name = name;
        m_default_alias = default_alias;
    }
    
    /**
     * Private constructor.
     */
    protected AbstractVariable()
    {
        
    }
    
    @Override
    public String getName()
    {
        return m_name;
    }
    
    @Override
    public String getDefaultProjectAlias()
    {
        return m_default_alias;
    }
    
    /**
     * Sets the default alias of the variable to be used in a Project operator when an explicit alias is not provided.
     * 
     * @param default_alias
     *            the default alias.
     */
    public void setDefaultProjectAlias(String default_alias)
    {
        assert (default_alias != null);
        m_default_alias = default_alias;
    }
    
    @Override
    public boolean isAnonymous()
    {
        return (m_name.startsWith(NameGenerator.NAME_PREFIX) && m_name.substring(2).matches("[0-9]*"));
    }
    
    @Override
    public int getBindingIndex()
    {
        return m_binding_index;
    }
    
    @Override
    public void setBindingIndex(int index)
    {
        m_binding_index = index;
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        return Collections.<Variable> singletonList(this);
    }
    
    @Override
    public List<Parameter> getBoundParametersUsed()
    {
        return Collections.emptyList();
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        return Collections.emptyList();
    }
    
    @Override
    public String toExplainString()
    {
        return m_name;
    }
    
    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof AbstractVariable)) return false;
        
        return m_name.equals(((AbstractVariable) other).m_name);
    }
    
    @Override
    public int hashCode()
    {
        return m_name.hashCode();
    }
    
    @Override
    public String toString()
    {
        return this.toExplainString();
    }
    
    /**
     * Copies common attributes to a variable copy.
     * 
     * @param copy
     *            a variable copy.
     */
    protected void copy(AbstractVariable copy)
    {
        copy.setBindingIndex(m_binding_index);
        super.copy(copy);
    }
    
}
