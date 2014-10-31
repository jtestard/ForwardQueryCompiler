/**
 * 
 */
package edu.ucsd.forward.fpl.ast;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;

/**
 * An abstract implementation of the statement.
 * 
 * @author Yupeng
 * 
 */
public abstract class AbstractStatement extends AbstractFplConstruct implements Statement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractStatement.class);
    
    /**
     * Constructor.
     * 
     * @param location
     *            a location.
     */
    public AbstractStatement(Location location)
    {
        super(location);
    }
    
}
