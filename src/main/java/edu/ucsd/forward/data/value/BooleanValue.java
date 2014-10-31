/**
 * 
 */
package edu.ucsd.forward.data.value;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.BooleanType;

/**
 * A value representing a boolean.
 * 
 * @author Keith Kowalczykowski
 * 
 */
@SuppressWarnings("serial")
public class BooleanValue extends AbstractPrimitiveValue<BooleanType, Boolean>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(BooleanValue.class);
    
    private boolean             m_bool;
    
    /**
     * Default constructor.
     */
    protected BooleanValue()
    {
        
    }
    
    /**
     * Constructs a new boolean value.
     * 
     * @param bool
     *            The boolean value.
     */
    public BooleanValue(boolean bool)
    {
        m_bool = bool;
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.value.AbstractValue#getTypeClass()
     */
    @Override
    public Class<BooleanType> getTypeClass()
    {
        return BooleanType.class;
    }
    
    @Override
    public Boolean getObject()
    {
        return m_bool;
    }
    
}
