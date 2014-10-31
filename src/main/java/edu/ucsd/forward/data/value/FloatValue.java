/**
 * 
 */
package edu.ucsd.forward.data.value;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.FloatType;

/**
 * A float value.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class FloatValue extends AbstractNumericValue<FloatType, Float>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(FloatValue.class);
    
    private float               m_float;
    
    /**
     * Default constructor.
     */
    protected FloatValue()
    {
        
    }
    
    /**
     * Constructs a float value with the given float.
     * 
     * @param f
     *            the given float
     */
    public FloatValue(float f)
    {
        m_float = f;
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.value.AbstractValue#getTypeClass()
     */
    @Override
    public Class<? extends FloatType> getTypeClass()
    {
        return FloatType.class;
    }
    
    @Override
    public Float getObject()
    {
        return m_float;
    }
    
}
