/**
 * 
 */
package edu.ucsd.forward.query.function.aggregate;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.function.AbstractFunction;

/**
 * An abstract implementation of the aggregate function interface. For all the aggregate functions except COUNT(*), all input the
 * bindings from which the term is evaluated to be null are ignored.
 * 
 * @author Yupeng
 * 
 */
public abstract class AbstractAggregateFunction extends AbstractFunction implements AggregateFunction
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractAggregateFunction.class);
    
    /**
     * Constructor.
     * 
     * @param name
     *            the name of the function.
     */
    protected AbstractAggregateFunction(String name)
    {
        super(name);
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return true;
    }
}
