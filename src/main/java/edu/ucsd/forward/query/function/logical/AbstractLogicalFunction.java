/**
 * 
 */
package edu.ucsd.forward.query.function.logical;

import edu.ucsd.forward.query.function.Function;
import edu.ucsd.forward.query.function.comparison.AbstractComparisonFunction;

/**
 * An abstract implementation of the logical function definition interface.
 * 
 * @author Yupeng
 * 
 */
public abstract class AbstractLogicalFunction extends AbstractComparisonFunction implements LogicalFunction
{
    /**
     * Constructor.
     * 
     * @param name
     *            the name of the function.
     */
    protected AbstractLogicalFunction(String name)
    {
        super(name);
    }
    
    @Override
    public Notation getNotation()
    {
        return Function.Notation.INFIX;
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return true;
    }
    
}
