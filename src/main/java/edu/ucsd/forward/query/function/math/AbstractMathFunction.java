/**
 * 
 */
package edu.ucsd.forward.query.function.math;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.function.AbstractFunction;

/**
 * An abstract implementation of the math function definition interface.
 * 
 * @author Yupeng
 * 
 */
public abstract class AbstractMathFunction extends AbstractFunction implements MathFunction
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractMathFunction.class);
    
    /**
     * Constructor.
     * 
     * @param name
     *            the name of the function.
     */
    protected AbstractMathFunction(String name)
    {
        super(name);
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return true;
    }
}
