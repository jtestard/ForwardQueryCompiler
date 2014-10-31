/**
 * 
 */
package edu.ucsd.forward.data.type;

import java.math.BigDecimal;

import edu.ucsd.forward.data.value.DecimalValue;

/**
 * A type representing a decimal type.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class DecimalType extends AbstractNumericType<DecimalValue, BigDecimal>
{
    public DecimalType()
    {
    }
    
    @Override
    protected String getTypeName()
    {
        return TypeEnum.DECIMAL.getName();
    }
}
