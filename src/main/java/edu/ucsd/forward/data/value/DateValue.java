/**
 * 
 */
package edu.ucsd.forward.data.value;

import java.sql.Date;
import java.text.DateFormat;
import java.util.Locale;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.DateType;

/**
 * A value representing a date.
 * 
 * @author Keith Kowalczykowski
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class DateValue extends AbstractPrimitiveValue<DateType, Date>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DateValue.class);
    
    private Date                m_date;
    
    /**
     * Constructs a new date value with the given date.
     * 
     * @param date
     *            The date to use.
     */
    public DateValue(Date date)
    {
        assert (date != null);
        m_date = date;
    }
    
    /**
     * Default constructor.
     */
    protected DateValue()
    {
        
    }
    
    @Override
    public Date getObject()
    {
        return m_date;
    }
    
    @Override
    public Class<DateType> getTypeClass()
    {
        return DateType.class;
    }
    
    @Override
    public String toString()
    {
        DateFormat format = DateFormat.getDateInstance(DateFormat.MEDIUM, Locale.US);
        return format.format(m_date);
    }
    
}
