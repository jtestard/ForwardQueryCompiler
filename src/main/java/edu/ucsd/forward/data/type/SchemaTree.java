/**
 * 
 */
package edu.ucsd.forward.data.type;

import java.util.Arrays;
import java.util.List;

import edu.ucsd.forward.data.constraint.Constraint;
import edu.ucsd.forward.data.source.DataSource;

/**
 * A schema tree.
 * 
 * @author Kian Win
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class SchemaTree extends AbstractComplexType
{
    
    private Type m_root_type;
    
    /**
     * Constructs a schema tree.
     * 
     * @param root_type
     *            the root type.
     */
    public SchemaTree(Type root_type)
    {
        setRootType(root_type);
    }
    
    /**
     * Default constructor.
     */
    protected SchemaTree()
    {
        
    }
    
    /**
     * Returns the root type.
     * 
     * @return the root type.
     */
    public Type getRootType()
    {
        return m_root_type;
    }
    
    /**
     * Sets the root type. Setting the root type to <code>null</code> will detach the existing root type (if any).
     * 
     * @param root_type
     *            the root type.
     */
    @SuppressWarnings("unchecked")
    public void setRootType(Type root_type)
    {
        // Detach the old root type
        if (m_root_type != null)
        {
            ((AbstractType) m_root_type).setParent(null);
        }
        
        // Attach the new root type
        m_root_type = root_type;
        if (m_root_type != null)
        {
            ((AbstractType) m_root_type).setParent(this);
        }
    }
    
    @Override
    public List<Type> getChildren()
    {
        return Arrays.asList(m_root_type);
    }
    
    @Override
    public void addConstraint(Constraint constraint)
    {
        // We no longer keep constraints at schema root
        assert (false);
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        getRootType().toQueryString(sb, tabs, data_source);
    }
    
}
