/**
 * 
 */
package edu.ucsd.forward.query.function.string;

import edu.ucsd.forward.query.function.AbstractFunction;

/**
 * An abstract implementation of the string function definition interface.
 * 
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractStringFunction extends AbstractFunction implements StringFunction
{
    /**
     * Constructor.
     * 
     * @param name
     *            the name of the function.
     */
    protected AbstractStringFunction(String name)
    {
        super(name);
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return true;
    }
    
}
