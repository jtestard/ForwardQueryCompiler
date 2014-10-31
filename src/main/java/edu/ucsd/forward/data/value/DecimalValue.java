/**
 * 
 */
package edu.ucsd.forward.data.value;

import java.math.BigDecimal;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.DecimalType;

/**
 * A value representing a decimal, which maps to NUMERIC and DECIMAL in SQL.
 * 
 * @author Yupeng
 */
@SuppressWarnings("serial")
public class DecimalValue extends AbstractNumericValue<DecimalType, BigDecimal>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DecimalValue.class);
    
    private BigDecimal          m_decimal;
    
    /**
     * Constructs a new decimal value with the given decimal.
     * 
     * @param d
     *            The decimal to initialize this value with.
     */
    public DecimalValue(BigDecimal d)
    {
        m_decimal = d;
    }
    
    /**
     * Default constructor.
     */
    protected DecimalValue()
    {
        
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.value.AbstractValue#getTypeClass()
     */
    @Override
    public Class<? extends DecimalType> getTypeClass()
    {
        return DecimalType.class;
    }
    
    @Override
    public BigDecimal getObject()
    {
        return m_decimal;
    }
    
}
