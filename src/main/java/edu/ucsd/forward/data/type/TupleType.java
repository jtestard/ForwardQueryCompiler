/**
 * 
 */
package edu.ucsd.forward.data.type;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import edu.ucsd.app2you.util.collection.SmallMap;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.constraint.ConstraintUtil;
import edu.ucsd.forward.data.source.DataSource;

/**
 * A tuple type.
 * 
 * @author Kian Win
 * @author Yupeng
 * @author Michalis Petropoulos
 */
@SuppressWarnings("serial")
public class TupleType extends AbstractComplexType implements Iterable<TupleType.AttributeEntry>
{
    @SuppressWarnings("unused")
    private static final Logger    log = Logger.getLogger(TupleType.class);
    
    private SmallMap<String, Type> m_attributes;
    
    /**
     * Indicates if the tuple should be inlined into the containing tuple value.
     */
    private boolean                m_inline;
    
    /**
     * Constructs a tuple type.
     * 
     */
    public TupleType()
    {
        // Optimization: use SmallMap to reduce memory usage
        m_attributes = new SmallMap<String, Type>();
    }
    
    /**
     * Checks if the tuple type is set as inline.
     * 
     * @return whether the tuple type is set as inline.
     */
    public boolean isInline()
    {
        return m_inline;
    }
    
    /**
     * Sets the tuple type as inline.
     * 
     * @param inline
     *            inline value.
     */
    public void setInline(boolean inline)
    {
        m_inline = inline;
    }
    
    @Override
    public Collection<Type> getChildren()
    {
        return Collections.unmodifiableCollection(m_attributes.values());
    }
    
    /**
     * Returns the number of attributes.
     * 
     * @return the number of attributes.
     */
    public int getSize()
    {
        return m_attributes.size();
    }
    
    /**
     * Returns the attribute names.
     * 
     * @return the attribute names.
     */
    public Set<String> getAttributeNames()
    {
        return Collections.unmodifiableSet(m_attributes.keySet());
    }
    
    @Override
    public Iterator<AttributeEntry> iterator()
    {
        return new Iterator<TupleType.AttributeEntry>()
        {
            private Iterator<Entry<String, Type>> m_iterator = m_attributes.entrySet().iterator();
            
            @Override
            public boolean hasNext()
            {
                return m_iterator.hasNext();
            }
            
            @Override
            public AttributeEntry next()
            {
                Entry<String, Type> entry = m_iterator.next();
                return new AttributeEntry(entry.getKey(), entry.getValue());
            }
            
            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
    
    /**
     * AttributeEntry class used for iterating a tuple type.
     * 
     * We do NOT use Map.Entry because it allows setValue() while we want to guard such change.
     * 
     * @author Kevin Zhao
     * 
     */
    public static class AttributeEntry
    {
        private String m_name;
        
        private Type   m_type;
        
        /**
         * Constructs with given name and type.
         * 
         * @param name
         *            the name
         * @param type
         *            the type
         */
        public AttributeEntry(String name, Type type)
        {
            m_name = name;
            m_type = type;
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
        
        /**
         * Returns the type.
         * 
         * @return the type.
         */
        public Type getType()
        {
            return m_type;
        }
        
        @Override
        public String toString()
        {
            return getName() + ": " + getType().toString();
        }
    }
    
    /**
     * Returns the attribute type corresponding to the name.
     * 
     * @param attribute_name
     *            - the attribute name.
     * @return the type of the attribute if the name is valid; <code>null</code> otherwise.
     */
    public Type getAttribute(String attribute_name)
    {
        assert (attribute_name != null) : "Name must be non-null";
        return m_attributes.get(attribute_name);
    }
    
    /**
     * Determines if the tuple type has an attribute with the given name.
     * 
     * @param attribute_name
     *            the attribute name.
     * @return true if the tuple type has an attribute with the given name; false, otherwise.
     */
    public boolean hasAttribute(String attribute_name)
    {
        assert (attribute_name != null);
        return m_attributes.containsKey(attribute_name);
    }
    
    /**
     * Sets the attribute type corresponding to the name.
     * 
     * @param attribute_name
     *            - the attribute name.
     * @param type
     *            - the attribute type.
     */
    public void setAttribute(String attribute_name, Type type)
    {
        this.setAttribute(attribute_name, type, true);
    }
    
    /**
     * Sets the attribute type corresponding to the name without checking if the attribute name is valid. This method is used in the
     * query processor for performance reasons.
     * 
     * @param attribute_name
     *            - the attribute name.
     * @param type
     *            - the attribute type.
     */
    public void setAttributeNoCheck(String attribute_name, Type type)
    {
        this.setAttribute(attribute_name, type, false);
    }
    
    /**
     * Sets the attribute type corresponding to the name.
     * 
     * @param attribute_name
     *            - the attribute name.
     * @param type
     *            - the attribute type.
     * @param name_check
     *            - determines whether an attribute name validity check should be performed.
     */
    private void setAttribute(String attribute_name, Type type, boolean name_check)
    {
        assert (attribute_name != null) : "Name must be non-null";
        assert (type != null) : "Type must be non-null";
        if (name_check)
        {
            assert (TypeUtil.isValidAttributeName(attribute_name)) : "Invalid attribute name: " + attribute_name;
        }
        
        if (type instanceof TupleType && ((TupleType) type).isInline())
        {
            setInlineTupleAttribute((TupleType) type, name_check);
        }
        else if (type instanceof SwitchType && ((SwitchType) type).isInline())
        {
            setInlineSwitchAttribute(attribute_name, (SwitchType) type, name_check);
        }
        else
        {
            Type old = m_attributes.put(attribute_name, type);
            
            // Same type - nothing to be done
            if (old == type) return;
            
            // If there was an existing type, detach it
            if (old != null)
            {
                ((AbstractType<?>) old).setParent(null);
            }
            
            setParent(this, type);
        }
    }
    
    private void setInlineTupleAttribute(TupleType tuple, boolean name_check)
    {
        for (String name : tuple.getAttributeNames())
        {
            setAttribute(name, TypeUtil.cloneNoParent(tuple.getAttribute(name)), name_check);
        }
    }
    
    private void setInlineSwitchAttribute(String name, SwitchType switch_type, boolean name_check)
    {
        Type union_type = TypeUtil.deepUnionSwitchType(switch_type);
        
        // the string type denoting the selected case name.
        setAttribute(SwitchType.getSelectedCasePrefix() + name, new StringType(), name_check);
        
        setAttribute(name, union_type);
    }
    
    /**
     * Sets the parent of a value type.
     * 
     * @param parent
     *            the tuple type parent.
     * @param child
     *            the value type.
     */
    @SuppressWarnings("unchecked")
    private static void setParent(TupleType parent, Type child)
    {
        /*
         * setParent() is defined on the abstract class so that it can be a protected method. However, casting the value to the
         * genericized abstract class causes a generics warning because of type erasure.
         */
        ((AbstractType<TupleType>) child).setParent(parent);
    }
    
    /**
     * Removes the attribute type corresponding to the name.
     * 
     * @param attribute_name
     *            - the attribute hanme.
     * @return the attribute type if it existed; <code>null</code> otherwise.
     */
    public Type removeAttribute(String attribute_name)
    {
        assert (attribute_name != null);
        
        Type old = m_attributes.remove(attribute_name);
        
        // If there was an existing type, detach it
        if (old != null)
        {
            ((AbstractType<?>) old).setParent(null);
        }
        
        return old;
    }
    
    /**
     * Removes the attribute type corresponding to the value type.
     * 
     * @param type
     *            - the attribute type.
     * @return the attribute type if it existed; <code>null</code> otherwise.
     */
    public Type removeAttribute(Type type)
    {
        String attribute_name = getAttributeName(type);
        return removeAttribute(attribute_name);
    }
    
    /**
     * Returns the attribute name corresponding to a type.
     * 
     * @param value_type
     *            - the type.
     * @return the attribute name if the type exists; <code>null</code> otherwise.
     */
    public String getAttributeName(Type value_type)
    {
        for (Map.Entry<String, Type> entry : m_attributes.entrySet())
        {
            // Use object equality
            if (entry.getValue() == value_type) return entry.getKey();
        }
        
        return null;
    }
    
    @Override
    public String toString()
    {
        /*
         * Example:
         * 
         * {attribute_name_1 : type_1, attribute_name_2 : type_2}
         */
        
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        
        boolean first = true;
        for (Map.Entry<String, Type> entry : m_attributes.entrySet())
        {
            if (first)
            {
                first = false;
            }
            else
            {
                sb.append(", ");
            }
            
            sb.append(entry.getKey().toString());
            sb.append(" : ");
            sb.append(entry.getValue().toString());
        }
        
        sb.append("}");
        return sb.toString();
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(TypeEnum.TUPLE.getName() + "(\n");
        boolean first = true;
        for (Map.Entry<String, Type> entry : m_attributes.entrySet())
        {
            if (first)
            {
                first = false;
            }
            else
            {
                sb.append(",\n");
            }
            
            sb.append(((AbstractType<Type>) entry.getValue()).getIndent() + entry.getKey() + " ");
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
