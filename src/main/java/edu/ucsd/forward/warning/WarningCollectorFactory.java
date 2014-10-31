/**
 * 
 */
package edu.ucsd.forward.warning;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A factory that produces warning collector per thread.
 * 
 * @author Yupeng
 * 
 */
public final class WarningCollectorFactory
{
    @SuppressWarnings("unused")
    private static final Logger                        log      = Logger.getLogger(WarningCollectorFactory.class);
    
    /**
     * The singleton name warning collector per thread.
     */
    private static final ThreadLocal<WarningCollector> INSTANCE = new ThreadLocal<WarningCollector>();
    
    /**
     * Hidden constructor.
     */
    private WarningCollectorFactory()
    {
        
    }
    
    /**
     * Gets the warning collector instance.
     * 
     * @return the warning collector instance.
     */
    public static WarningCollector getInstance()
    {
        WarningCollector collector = INSTANCE.get();
        if (collector == null)
        {
            collector = new WarningCollector();
            INSTANCE.set(collector);
        }
        return collector;
    }
}
