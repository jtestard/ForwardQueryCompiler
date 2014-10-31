/**
 * 
 */
package edu.ucsd.forward.query.ast;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;

/**
 * Represents an abstract implementation of the value expression interface.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractValueExpression extends AbstractQueryConstruct implements ValueExpression
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractValueExpression.class);
    
    /**
     * Constructor.
     * 
     * @param location
     *            the location of the term.
     */
    public AbstractValueExpression(Location location)
    {
        super(location);
    }
    
}
