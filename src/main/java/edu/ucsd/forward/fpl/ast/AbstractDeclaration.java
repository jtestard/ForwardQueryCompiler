/**
 * 
 */
package edu.ucsd.forward.fpl.ast;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;

/**
 * The abstract implementation of the action language's declaration.
 * 
 * @author Yupeng
 * 
 */
public abstract class AbstractDeclaration extends AbstractFplConstruct implements Declaration
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractDeclaration.class);
    
    /**
     * Constructor.
     * 
     * @param location
     *            a location.
     */
    public AbstractDeclaration(Location location)
    {
        super(location);
    }
    
}
