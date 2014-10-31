/**
 * 
 */
package edu.ucsd.forward.query.ast;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.fpl.ast.AbstractFplConstruct;

/**
 * The abstract implementation of query construct.
 * 
 * @author Yupeng
 * 
 */
public abstract class AbstractQueryConstruct extends AbstractFplConstruct implements QueryStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractQueryConstruct.class);
    
    /**
     * Constructor.
     * 
     * @param location
     *            the location of the term.
     */
    public AbstractQueryConstruct(Location location)
    {
        super(location);
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        toQueryString(sb, 0, null);
        
        return sb.toString();
    }
    
    @Override
    public void toActionString(StringBuilder sb, int tabs)
    {
        toQueryString(sb, tabs, null);
    }
}
