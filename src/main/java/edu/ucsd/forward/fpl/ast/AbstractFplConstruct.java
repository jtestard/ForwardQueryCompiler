/**
 * 
 */
package edu.ucsd.forward.fpl.ast;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.ast.AbstractAstNode;

/**
 * Represents the abstract implementation of the action construct.
 * 
 * @author Yupeng
 * 
 */
public abstract class AbstractFplConstruct extends AbstractAstNode implements FplConstruct
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractFplConstruct.class);
    
    /**
     * Constructor.
     * 
     * @param location
     *            a location.
     */
    public AbstractFplConstruct(Location location)
    {
        super(location);
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        toActionString(sb, 0);
        return sb.toString();
    }
    
}
