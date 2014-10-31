/**
 * 
 */
package edu.ucsd.forward.data.value;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Locale;

import edu.ucsd.forward.data.type.TimestampType;

/**
 * A value representing a timestamp.
 * 
 * @author Keith Kowalczykowski
 */
@SuppressWarnings("serial")
public class TimestampValue extends AbstractPrimitiveValue<TimestampType, Timestamp>
{
    
    private Timestamp m_timestamp;
    
    /**
     * Constructs a new timestamp value.
     * 
     * @param timestamp
     *            The date object representing the timestamp.
     */
    public TimestampValue(Timestamp timestamp)
    {
        assert (timestamp != null);
        m_timestamp = timestamp;
    }
    
    /**
     * Default constructor.
     */
    protected TimestampValue()
    {
        
    }
    
    @Override
    public Class<? extends TimestampType> getTypeClass()
    {
        return TimestampType.class;
    }
    
    @Override
    public String toString()
    {
        DateFormat format = DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.FULL, Locale.US);
        return format.format(m_timestamp);
    }
    
    @Override
    public Timestamp getObject()
    {
        return m_timestamp;
    }
    
    /**
     * Parses literal to construct timestamp value.
     * 
     * @param literal
     *            the input literal
     * @return the constructed timestamp value.
     * @throws ParseException
     *             anything wrong with the parsing.
     */
    public static TimestampValue parse(String literal) throws ParseException
    {
        long millisecs = DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.FULL, Locale.US).parse(literal).getTime();
        // long millisecs = javax.xml.bind.DatatypeConverter.parseDateTime(literal).getTimeInMillis();
        return new TimestampValue(new Timestamp(millisecs));
    }
}
