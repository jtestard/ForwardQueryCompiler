/**
 * 
 */
package edu.ucsd.forward.data.index;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.value.ScalarValue;

/**
 * A key range for retrieving records from index.
 * 
 * @author Yupeng
 * 
 */
public final class KeyRange
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(KeyRange.class);
    
    private ScalarValue         m_lower;
    
    private ScalarValue         m_upper;
    
    private boolean             m_lower_open;
    
    private boolean             m_upper_open;
    
    /**
     * Private constructor.
     */
    private KeyRange()
    {
        
    }
    
    /**
     * Gets the lower bound.
     * 
     * @return the lower bound.
     */
    public ScalarValue getLowerBound()
    {
        return m_lower;
    }
    
    /**
     * Gets the upper bound.
     * 
     * @return the upper bound.
     */
    public ScalarValue getUpperBound()
    {
        return m_upper;
    }
    
    /**
     * Checks if the lower bound value is included.
     * 
     * @return <code>true</code> if the lower bound value is included, <code>false</code> otherwise.
     */
    public boolean isLowerOpen()
    {
        return m_lower_open;
    }
    
    /**
     * Checks if the upper bound value is included.
     * 
     * @return <code>true</code> if the upper bound value is included, <code>false</code> otherwise.
     */
    public boolean isUpperOpen()
    {
        return m_upper_open;
    }
    
    /**
     * Creates a new key range with both lower and upper set to the specified value and both lowerOpen and upperOpen set to false.
     * 
     * @param value
     *            the specified value.
     * @return the created range.
     */
    public static KeyRange only(ScalarValue value)
    {
        return bound(value, value, false, false);
    }
    
    /**
     * Creates a new key range with lower bound.
     * 
     * @param lower
     *            lower bound value.
     * @param lower_open
     *            indicates if the lower bound value is included.
     * @return a new key range with lower bound.
     */
    public static KeyRange lowerBound(ScalarValue lower, boolean lower_open)
    {
        return bound(lower, null, lower_open, false);
    }
    
    /**
     * Creates a new key range with upper bound.
     * 
     * @param upper
     *            upper bound value
     * @param upper_open
     *            indicates if the upper bound value is included.
     * @return a new key range with upper bound.
     */
    public static KeyRange upperBound(ScalarValue upper, boolean upper_open)
    {
        return bound(null, upper, false, upper_open);
    }
    
    /**
     * Creates a new key range with lower and upper bound.
     * 
     * @param lower
     *            lower bound value
     * @param upper
     *            upper bound value
     * @param lower_open
     *            indicates if the lower bound value is included.
     * @param upper_open
     *            indicates if the upper bound value is included.
     * @return a new key range with lower and upper bound.
     */
    public static KeyRange bound(ScalarValue lower, ScalarValue upper, boolean lower_open, boolean upper_open)
    {
        KeyRange range = new KeyRange();
        range.m_lower = lower;
        range.m_upper = upper;
        range.m_lower_open = lower_open;
        range.m_upper_open = upper_open;
        return range;
    }
}
