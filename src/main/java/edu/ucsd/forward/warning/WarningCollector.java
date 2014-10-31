/**
 * 
 */
package edu.ucsd.forward.warning;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A collector that collects warnings during application compilation.
 * 
 * @author Yupeng
 * 
 */
public class WarningCollector
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(WarningCollector.class);
    
    private List<Warning>       m_warnings;
    
    /**
     * Hidden constructor.
     */
    protected WarningCollector()
    {
        m_warnings = new ArrayList<Warning>();
    }
    
    /**
     * Adds a warning.
     * 
     * @param warning
     *            the warning to add.
     */
    public void add(Warning warning)
    {
        assert warning != null;
        m_warnings.add(warning);
    }
    
    /**
     * Gets all the warnings collected.
     * 
     * @return all the warnings collected.
     */
    public List<Warning> getWarnings()
    {
        return new ArrayList<Warning>(m_warnings);
    }
    
    /**
     * Clears all the warnings.
     */
    public void clear()
    {
        m_warnings.clear();
    }
}
