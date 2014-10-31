/**
 * 
 */
package edu.ucsd.forward.data.value;

import java.util.Collection;
import java.util.Collections;

import edu.ucsd.forward.data.type.NullType;
import edu.ucsd.forward.data.type.Type;

/**
 * A null value.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class NullValue extends AbstractValue<Type, AbstractComplexValue<?>>
{
    
    public static final String    NULL = "null";
    
    private Class<? extends Type> m_type_class;
    
    /**
     * Constructor.
     */
    public NullValue()
    {
        super();
        
        m_type_class = NullType.class;
    }
    
    /**
     * Constructor.
     * 
     * @param type_class
     *            the type class of the null value.
     */
    public NullValue(Class<? extends Type> type_class)
    {
        super();
        
        assert (type_class != null);
        m_type_class = type_class;
    }
    
    @Override
    public Class<? extends Type> getTypeClass()
    {
        return m_type_class;
    }
    
    public void setTypeClass(Class<? extends Type> type_class)
    {
        assert type_class != null;
        m_type_class = type_class;
    }
    
    @Override
    public String toString()
    {
        return NULL;
    }
    
    @Override
    public Collection<? extends Value> getChildren()
    {
        return Collections.emptyList();
    }
    
}
