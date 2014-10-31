/**
 * 
 */
package edu.ucsd.forward.data.type;

import java.sql.Timestamp;

import edu.ucsd.forward.data.value.TimestampValue;

/**
 * A type representing a timestamp.
 * 
 * @author Keith Kowalczykowski
 * @author Yupeng
 * @author Michalis Petropoulos
 */
@SuppressWarnings("serial")
public class TimestampType extends AbstractPrimitiveType<TimestampValue, Timestamp>
{
    public TimestampType()
    {
    }
    
    @Override
    protected String getTypeName()
    {
        return TypeEnum.TIMESTAMP.getName();
    }
}
