/**
 * 
 */
package edu.ucsd.forward.query.ast;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;

/**
 * The abstract implementation of the query interface.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractQueryNode extends AbstractValueExpression implements QueryNode
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractQueryNode.class);
    
    /**
     * Constructor.
     * 
     * @param location
     *            a location.
     */
    public AbstractQueryNode(Location location)
    {
        super(location);
    }
    
}
