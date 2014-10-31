/**
 * 
 */
package edu.ucsd.forward.data.type;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import edu.ucsd.app2you.util.collection.SmallMap;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.constraint.ConstraintUtil;
import edu.ucsd.forward.data.source.DataSource;

/**
 * A switch type.
 * 
 * @author Kian Win
 * @author Yupeng
 * @author Michalis Petropoulos
 */
@SuppressWarnings("serial")
public class SwitchType extends AbstractComplexType
{
    @SuppressWarnings("unused")
    private static final Logger    log = Logger.getLogger(SwitchType.class);
    
    private SmallMap<String, Type> m_cases;
    
    /**
     * Indicates if the switch should be inlined into the containing tuple value.
     */
    private boolean                m_inline;
    
    /**
     * Constructs the switch type.
     * 
     */
    public SwitchType()
    {
        // Optimization: use SmallMap to reduce memory usage
        m_cases = new SmallMap<String, Type>();
        m_inline = false;
    }
    
    @Override
    public Collection<Type> getChildren()
    {
        return Collections.unmodifiableCollection(m_cases.values());
    }
    
    /**
     * Returns the case names.
     * 
     * @return the case names.
     */
    public Set<String> getCaseNames()
    {
        return Collections.unmodifiableSet(m_cases.keySet());
    }
    
    /**
     * Returns the case type corresponding to the name.
     * 
     * @param case_name
     *            - the case name.
     * @return the type of the case if the name is valid; <code>null</code> otherwise.
     */
    public Type getCase(String case_name)
    {
        assert (case_name != null) : "Case name must be non-null";
        
        return m_cases.get(case_name);
    }
    
    /**
     * Checks if the switch type is set as inline.
     * 
     * @return whether the switch type is set as inline.
     */
    public boolean isInline()
    {
        return m_inline;
    }
    
    /**
     * Sets the switch type as inline.
     * 
     * @param inline
     *            inline value.
     */
    public void setInline(boolean inline)
    {
        m_inline = inline;
    }
    
    /**
     * Gets the system generated prefix for the attribute denoting the selected name of the switch type in its inlined tuple.
     * 
     * @return the system generated prefix for the attribute denoting the selected name of the switch type in its inlined tuple.
     */
    public static String getSelectedCasePrefix()
    {
        return "__selected_";
    }
    
    /**
     * Sets the case type corresponding to the name.
     * 
     * @param case_name
     *            - the case name.
     * @param type
     *            - the case type.
     */
    @SuppressWarnings("unchecked")
    public void setCase(String case_name, Type type)
    {
        assert (case_name != null);
        assert (type != null);
        
        Type old = m_cases.put(case_name, type);
        
        // If there was an existing type, detach it
        if (old != null)
        {
            ((AbstractType<Type>) old).setParent(null);
        }
        
        ((AbstractType<Type>) type).setParent(this);
    }
    
    /**
     * Removes the type corresponding to the name.
     * 
     * @param case_name
     *            - the case name.
     * @return the type if it existed; <code>null</code> otherwise.
     */
    @SuppressWarnings("unchecked")
    public Type removeCase(String case_name)
    {
        assert (case_name != null);
        
        Type old = m_cases.remove(case_name);
        
        // If there was an existing type, detach it
        if (old != null)
        {
            ((AbstractType<Type>) old).setParent(null);
        }
        
        return old;
    }
    
    /**
     * Removes the type.
     * 
     * @param type
     *            - the type.
     * @return the type if it existed; <code>null</code> otherwise.
     */
    public Type removeCase(Type type)
    {
        assert (type != null);
        
        String case_name = getCaseName(type);
        return removeCase(case_name);
    }
    
    /**
     * Returns the case name corresponding to the type.
     * 
     * @param type
     *            the type.
     * @return the case name if the type exists; <code>null</code> otherwise.
     * 
     */
    public String getCaseName(Type type)
    {
        for (Entry<String, Type> entry : m_cases.entrySet())
        {
            // Use object equality
            if (entry.getValue() == type) return entry.getKey();
        }
        
        return null;
    }
    
    @Override
    public String toString()
    {
        /*
         * Example:
         * 
         * <case_name_1 : type_1 | case_name_2 : type_2>
         */
        
        StringBuilder sb = new StringBuilder();
        sb.append("<");
        
        boolean first = true;
        for (Map.Entry<String, Type> entry : m_cases.entrySet())
        {
            if (first)
            {
                first = false;
            }
            else
            {
                sb.append(" | ");
            }
            
            sb.append(entry.getKey().toString());
            sb.append(" : ");
            sb.append(entry.getValue().toString());
        }
        
        sb.append(">");
        return sb.toString();
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(TypeEnum.SWITCH.getName() + "(\n");
        boolean first = true;
        for (Map.Entry<String, Type> entry : m_cases.entrySet())
        {
            if (first)
            {
                first = false;
            }
            else
            {
                sb.append(",\n");
            }
            sb.append(((AbstractType<Type>) entry.getValue()).getIndent() + entry.getKey().toString() + " ");
            entry.getValue().toQueryString(sb, tabs, data_source);
        }
        sb.append("\n" + getIndent() + ")");
        // Display non null constraint
        if (ConstraintUtil.getNonNullConstraint(this) != null)
        {
            sb.append(" NOT NULL ");
        }
    }
}
