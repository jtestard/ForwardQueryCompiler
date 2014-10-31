/**
 * 
 */
package edu.ucsd.forward.data.value;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.DoubleType;

/**
 * A value representing a double.
 * 
 * @author Keith Kowalczykowski
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class DoubleValue extends AbstractNumericValue<DoubleType, Double>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DoubleValue.class);
    
    private double              m_double;
    
    /**
     * Default constructor.
     */
    protected DoubleValue()
    {
        
    }
    
    /**
     * Constructs a new double value with the given double.
     * 
     * @param d
     *            The double to initialize this value with.
     */
    public DoubleValue(double d)
    {
        m_double = d;
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.value.AbstractValue#getTypeClass()
     */
    @Override
    public Class<? extends DoubleType> getTypeClass()
    {
        return DoubleType.class;
    }
    
    @Override
    public Double getObject()
    {
        return m_double;
    }
    
}
