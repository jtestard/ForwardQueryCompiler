/**
 * 
 */
package edu.ucsd.forward.query.function.comparison;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.function.AbstractFunction;

/**
 * An abstract implementation of the comparison function definition interface.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractComparisonFunction extends AbstractFunction implements ComparisonFunction
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractComparisonFunction.class);
    
    /**
     * Constructor.
     * 
     * @param name
     *            the name of the function.
     */
    protected AbstractComparisonFunction(String name)
    {
        super(name);
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return true;
    }
    
}
