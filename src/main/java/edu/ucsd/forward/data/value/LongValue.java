/**
 * 
 */
package edu.ucsd.forward.data.value;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.LongType;

/**
 * A value representing a long, which maps to BIGINT in SQL.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class LongValue extends AbstractNumericValue<LongType, Long>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(LongValue.class);
    
    private long                m_long;
    
    /**
     * Default constructor.
     */
    protected LongValue()
    {
        
    }
    
    /**
     * Constructs a new long value with the given long.
     * 
     * @param l
     *            The long to initialize this value with.
     */
    public LongValue(long l)
    {
        m_long = l;
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.value.AbstractValue#getTypeClass()
     */
    @Override
    public Class<? extends LongType> getTypeClass()
    {
        return LongType.class;
    }
    
    @Override
    public Long getObject()
    {
        return m_long;
    }
    
}
