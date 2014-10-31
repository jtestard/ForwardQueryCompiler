/**
 * 
 */
package edu.ucsd.forward.data.value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.StringUtil;
import edu.ucsd.forward.data.type.CollectionType;

/**
 * A relation contains a set of homogeneous tuples.
 * 
 * @author Kian Win
 * @author Romain Vernoux
 */
@SuppressWarnings("serial")
public class CollectionValue extends AbstractComplexValue<CollectionType>
{
    
    private boolean     m_ordered = false;
    
    private List<Value> m_values;
    
    // FIXME unmodifiable list is not GWT serializable
    // private List<TupleValue> m_tuples_unmodifiable;
    
    /**
     * Constructs a relation.
     * 
     */
    public CollectionValue()
    {
        m_values = new ArrayList<Value>();
        // m_tuples_unmodifiable = Collections.unmodifiableList(m_tuples);
    }
    
    /**
     * Checks if the relation has tuples.
     * 
     * @return <code>true</code> if the relation has tuples; <code>false</code> otherwise
     */
    public boolean isEmpty()
    {
        return m_values.isEmpty();
    }
    
    /**
     * Tells if this collection is a list or a bag.
     * 
     * @return <code>true</code> if this collection is a list, <code>false</code> otherwise.
     */
    public boolean isOrdered()
    {
        return m_ordered;
    }
    
    /**
     * Sets the flag telling if this collection is a list or a bag.
     * 
     * @param flag
     *            should be <code>true</code> if this collection is a list, <code>false</code> otherwise.
     */
    public void setOrdered(boolean flag)
    {
        m_ordered = flag;
    }
    
    @Override
    public Class<CollectionType> getTypeClass()
    {
        return CollectionType.class;
    }
    
    @Override
    @Deprecated
    public CollectionType getType()
    {
        // Override super method to give more specific return signature
        return ((CollectionType) super.getType());
    }
    
    @Override
    public List<Value> getChildren()
    {
        return Collections.unmodifiableList(m_values);
    }
    
    /**
     * Returns the tuples.
     * 
     * @return the tuples.
     */
    @Deprecated
    public List<TupleValue> getTuples()
    {
        ArrayList<TupleValue> tuples = new ArrayList<TupleValue>(m_values.size());
        for (Value v : m_values)
        {
            assert (v instanceof TupleValue);
            tuples.add((TupleValue) v);
        }
        return Collections.unmodifiableList(tuples);
    }
    
    /**
     * Returns the values in this collection
     * 
     * @return the values.
     */
    public List<Value> getValues()
    {
        return Collections.unmodifiableList(m_values);
    }
    
    /**
     * Adds the tuple to the end of the relation.
     * 
     * @param tuple
     *            - the tuple.
     */
    @Deprecated
    public void add(TupleValue tuple)
    {
        assert (tuple != null);
        
        m_values.add(tuple);
        tuple.setParent(this);
    }
    
    /**
     * Adds the value to the end of the relation.
     * 
     * @param value
     *            - the value.
     */
    @SuppressWarnings("unchecked")
    public void add(Value value)
    {
        assert (value != null);
        
        m_values.add(value);
        
        /*
         * setParent() is defined on the abstract class so that it can be a protected method. However, casting the value to the
         * genericized abstract class causes a generics warning because of type erasure.
         */
        ((AbstractValue<?, CollectionValue>) value).setParent(this);
    }
    
    /**
     * Appends the specific relation to the end of the current relation.
     * 
     * @param relation
     *            the relation to be appended
     */
    @SuppressWarnings("unchecked")
    public void append(CollectionValue relation)
    {
        if (relation == null) return;
        for (Value value : relation.getValues())
        {
            /*
             * setParent() is defined on the abstract class so that it can be a protected method. However, casting the value to the
             * genericized abstract class causes a generics warning because of type erasure.
             */
            ((AbstractValue<?, CollectionValue>) value).setParent(null);
            add(value);
        }
    }
    
    /**
     * Gets the size of tuples in the relation.
     * 
     * @return the size of tuples inside
     */
    public int size()
    {
        return m_values.size();
    }
    
    /**
     * Adds the tuple at the specified position.
     * 
     * @param index
     *            - the 0-based index at which the tuple will be inserted.
     * @param tuple
     *            - the tuple.
     */
    @Deprecated
    public void add(int index, TupleValue tuple)
    {
        assert (tuple != null) : "The tuple to be added cannot be null.";
        
        m_values.add(index, tuple);
        tuple.setParent(this);
    }
    
    /**
     * Adds the value at the specified position.
     * 
     * @param index
     *            - the 0-based index at which the tuple will be inserted.
     * @param value
     *            - the value.
     */
    @SuppressWarnings("unchecked")
    public void add(int index, Value value)
    {
        assert (value != null) : "The value to be added cannot be null.";
        
        m_values.add(index, value);
        /*
         * setParent() is defined on the abstract class so that it can be a protected method. However, casting the value to the
         * genericized abstract class causes a generics warning because of type erasure.
         */
        ((AbstractValue<?, CollectionValue>) value).setParent(this);
    }
    
    /**
     * Removes the tuple at the specified position.
     * 
     * @param index
     *            - the 0-based index from which the tuple will be removed.
     * @return the tuple that was removed.
     */
    @Deprecated
    public TupleValue remove(int index)
    {
        Value value = m_values.remove(index);
        assert (value instanceof TupleValue);
        TupleValue tuple = (TupleValue) value;
        tuple.setParent(null);
        return tuple;
    }
    
    /**
     * Removes the value at the specified position.
     * 
     * @param index
     *            - the 0-based index from which the tuple will be removed.
     * @return the value that was removed.
     */
    @SuppressWarnings("unchecked")
    public Value removeValue(int index)
    {
        Value value = m_values.remove(index);
        /*
         * setParent() is defined on the abstract class so that it can be a protected method. However, casting the value to the
         * genericized abstract class causes a generics warning because of type erasure.
         */
        ((AbstractValue<?, CollectionValue>) value).setParent(null);
        return value;
    }
    
    /**
     * Removes the specified tuple.
     * 
     * @param tuple
     *            - the tuple.
     * @return <code>true</code> if the tuple was found and removed; <code>false</code> otherwise.
     */
    @Deprecated
    public boolean remove(TupleValue tuple)
    {
        assert (tuple != null) : "The tuple to be removed cannot be null.";
        
        boolean found = m_values.remove(tuple);
        if (found)
        {
            tuple.setParent(null);
        }
        
        return found;
    }
    
    /**
     * Removes the specified value.
     * 
     * @param value
     *            - the value.
     * @return <code>true</code> if the tuple was found and removed; <code>false</code> otherwise.
     */
    @SuppressWarnings("unchecked")
    public boolean remove(Value value)
    {
        assert (value != null) : "The value to be removed cannot be null.";
        
        boolean found = m_values.remove(value);
        if (found)
        {
            /*
             * setParent() is defined on the abstract class so that it can be a protected method. However, casting the value to the
             * genericized abstract class causes a generics warning because of type erasure.
             */
            ((AbstractValue<?, CollectionValue>) value).setParent(null);
        }
        
        return found;
    }
    
    /**
     * Removes all tuples from this collection.
     */
    public void removeAll()
    {
        while (!isEmpty())
        {
            removeValue(0);
        }
    }
    
    @Override
    public String toString()
    {
        String indent_less = StringUtil.leftPad("", getHeight() - 2);
        String indent = StringUtil.leftPad("", getHeight() - 1);
        String indent_more = StringUtil.leftPad("", getHeight());
        
        /*-
         * Example:
         *  [ 
         *   tuple_1, 
         *   tuple_2 
         *  ]
         */
        
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append(indent);
        String open;
        String close;
        if(m_ordered)
        {
            open = "[";
            close = "]";
        }
        else
        {
            open = "{{";
            close = "}}";
        }
        sb.append(open + "\n");
        
        boolean first = true;
        for (Value value : m_values)
        {
            if (first)
            {
                first = false;
            }
            else
            {
                sb.append(",\n");
            }
            
            sb.append(indent_more);
            sb.append(value);
        }
        
        if (!first) sb.append("\n");
        sb.append(indent);
        sb.append(close + "\n");
        sb.append(indent_less);
        return sb.toString();
    }
    
}
