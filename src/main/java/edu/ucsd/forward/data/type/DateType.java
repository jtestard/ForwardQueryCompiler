/**
 * 
 */
package edu.ucsd.forward.data.type;

import java.sql.Date;

import edu.ucsd.forward.data.value.DateValue;

/**
 * A type representing dates.
 * 
 * @author Keith Kowalczykowski
 * @author Yupeng
 * @author Michalis Petropoulos
 */
@SuppressWarnings("serial")
public class DateType extends AbstractPrimitiveType<DateValue, Date>
{
    public DateType()
    {
    }
    
    @Override
    protected String getTypeName()
    {
        return TypeEnum.DATE.getName();
    }
    
}
