/**
 * 
 */
package edu.ucsd.forward.data.value;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import edu.ucsd.app2you.util.collection.SmallMap;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.TupleType;

/**
 * A tuple maps attribute names to values.
 * 
 * @author Kian Win
 * @author Michalis Petropoulos
 * @author Yupeng Fu
 * 
 */
@SuppressWarnings("serial")
public class TupleValue extends AbstractComplexValue<TupleType> implements Iterable<TupleValue.AttributeValueEntry>
{
    @SuppressWarnings("unused")
    private static final Logger     log = Logger.getLogger(TupleValue.class);
    
    private SmallMap<String, Value> m_attributes;
    
    /**
     * Indicates if the tuple should be inlined into the containing tuple value.
     */
    private boolean                 m_inline;
    
    @Override
    public Class<TupleType> getTypeClass()
    {
        return TupleType.class;
    }
    
    /**
     * Constructs the tuple.
     * 
     */
    public TupleValue()
    {
        // Optimization: use SmallMap to reduce memory usage
        m_attributes = new SmallMap<String, Value>();
    }
    
    @Override
    @Deprecated
    public TupleType getType()
    {
        // Override super method to give more specific return signature
        return (TupleType) super.getType();
    }
    
    @Override
    public Collection<Value> getChildren()
    {
        return Collections.unmodifiableCollection(m_attributes.values());
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
    public Iterator<TupleValue.AttributeValueEntry> iterator()
    {
        return new Iterator<TupleValue.AttributeValueEntry>()
        {
            private Iterator<Entry<String, Value>> m_iterator = m_attributes.entrySet().iterator();
            
            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public AttributeValueEntry next()
            {
                Entry<String, Value> entry = m_iterator.next();
                return new AttributeValueEntry(entry.getKey(), entry.getValue());
            }
            
            @Override
            public boolean hasNext()
            {
                return m_iterator.hasNext();
            }
        };
    }
    
    /**
     * AttributeValueEntry class used for iterating a tuple value.
     * 
     * We do NOT use Map.Entry because it allows setValue() while we want to guard such change.
     * 
     * @author Kevin Zhao
     * 
     */
    public static class AttributeValueEntry
    {
        private String m_name;
        
        private Value  m_value;
        
        /**
         * Constructs with given name and value.
         * 
         * @param name
         *            the name
         * @param value
         *            the value
         */
        public AttributeValueEntry(String name, Value value)
        {
            m_name = name;
            m_value = value;
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
         * Returns the value.
         * 
         * @return the value.
         */
        public Value getValue()
        {
            return m_value;
        }
    }
    
    /**
     * Returns the attribute value corresponding to the name.
     * 
     * @param attribute_name
     *            - the attribute name.
     * @return the value of the attribute if the name is valid; <code>null</code> otherwise.
     */
    public Value getAttribute(String attribute_name)
    {
        assert (attribute_name != null);
        return m_attributes.get(attribute_name);
    }
    
    /**
     * Returns the attribute value corresponding to the name, cast as the type of the provided <code>Class</code>.
     * 
     * @param attribute_name
     *            - the attribute name.
     * @param type
     *            The <code>Class</code> of the type of the attribute.
     * @param <V>
     *            A generic parameter representing the return type.
     * @return the value of the attribute if the name is valid; <code>null</code> otherwise.
     */
    @SuppressWarnings("unchecked")
    public <V extends Value> V getAttribute(String attribute_name, Class<V> type)
    {
        return (V) getAttribute(attribute_name);
    }
    
    /**
     * Sets the attribute value corresponding to the name. If there already exists a value corresponding to the name that is not the
     * given value, the old value will be detached. This method does not check the validity of the attribute name. It is the
     * responsibility of the caller to do so.
     * 
     * @param attribute_name
     *            - the attribute name.
     * @param value
     *            - the attribute value.
     */
    public void setAttribute(String attribute_name, Value value)
    {
        assert (attribute_name != null) : "Name must be non-null";
        assert (value != null) : "Value must be non-null";
        
        Value old = m_attributes.put(attribute_name, value);
        
        // Same value - nothing to be done
        if (old == value) return;
        
        // If there was an existing value, detach it
        if (old != null)
        {
            ((AbstractValue<?, ?>) old).setParent(null);
        }
        
        setParent(this, value);
    }
    
    /**
     * Sets the parent of a value.
     * 
     * @param parent
     *            the tuple parent.
     * @param child
     *            the value.
     */
    @SuppressWarnings("unchecked")
    private static void setParent(TupleValue parent, Value child)
    {
        /*
         * setParent() is defined on the abstract class so that it can be a protected method. However, casting the value to the
         * genericized abstract class causes a generics warning because of type erasure.
         */
        ((AbstractValue<?, TupleValue>) child).setParent(parent);
    }
    
    /**
     * Removes the attribute value corresponding to the name.
     * 
     * @param attribute_name
     *            - the attribute name.
     * @return the attribute value if it existed; <code>null</code> otherwise.
     */
    public Value removeAttribute(String attribute_name)
    {
        assert (attribute_name != null);
        
        Value old = m_attributes.remove(attribute_name);
        
        // If there was an existing value, detach it
        if (old != null)
        {
            ((AbstractValue<?, ?>) old).setParent(null);
        }
        
        return old;
    }
    
    /**
     * Removes the attribute value.
     * 
     * @param value
     *            - the attribute value.
     * @return the attribute value if it existed; <code>null</code> otherwise.
     */
    public Value removeAttribute(Value value)
    {
        String attribute_name = getAttributeName(value);
        return removeAttribute(attribute_name);
    }
    
    /**
     * Returns the attribute name corresponding to a value.
     * 
     * @param value
     *            - the value.
     * @return the attribute name.
     */
    public String getAttributeName(Value value)
    {
        for (Map.Entry<String, Value> entry : m_attributes.entrySet())
        {
            // Use object equality
            if (entry.getValue() == value) return entry.getKey();
        }
        
        return null;
    }
    
    @Override
    public String toString()
    {
        /*
         * Example:
         * 
         * {attribute_name_1 : value_1, attribute_name_2 : value_2}
         */
        
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        
        boolean first = true;
        for (Map.Entry<String, Value> entry : m_attributes.entrySet())
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
    
}
