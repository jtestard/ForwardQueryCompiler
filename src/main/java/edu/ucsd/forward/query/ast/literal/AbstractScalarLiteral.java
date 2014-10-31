/**
 * 
 */
package edu.ucsd.forward.query.ast.literal;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.exception.Location;

/**
 * Represents an abstract implementation of the scalar literal interface.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractScalarLiteral extends AbstractLiteral implements ScalarLiteral
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ScalarLiteral.class);
    
    private ScalarValue         m_value;
    
    /**
     * Creates an instance of the scalar literal.
     * 
     * @param literal
     *            the scalar literal.
     * @param location
     *            a location.
     */
    public AbstractScalarLiteral(String literal, Location location)
    {
        super(literal, location);
    }
    
    /**
     * Creates an instance of the scalar literal.
     * 
     * @param value
     *            the scalar value.
     * @param location
     *            a location.
     */
    public AbstractScalarLiteral(ScalarValue value, Location location)
    {
        super(value.toString(), location);
        
        assert (value != null);
        m_value = value;
    }
    
    @Override
    public ScalarValue getScalarValue()
    {
        return m_value;
    }
    
    @Override
    public void setScalarValue(ScalarValue scalar_value)
    {
        assert (scalar_value != null);
        m_value = scalar_value;
        
    }
    
}
