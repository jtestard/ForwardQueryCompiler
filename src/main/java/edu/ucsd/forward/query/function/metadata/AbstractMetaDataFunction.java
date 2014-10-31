/**
 * 
 */
package edu.ucsd.forward.query.function.metadata;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.function.AbstractFunction;

/**
 * An abstract implementation of the metadata function definition interface.
 * 
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractMetaDataFunction extends AbstractFunction implements MetaDataFunction
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractMetaDataFunction.class);
    
    /**
     * Constructor.
     * 
     * @param name
     *            the name of the function.
     */
    protected AbstractMetaDataFunction(String name)
    {
        super(name);
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return false;
    }
    
}
