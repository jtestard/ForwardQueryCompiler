package edu.ucsd.forward.query.logical.term;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A special type of relative variable that correspond to a position variable. Position variables correspond to the ordinal position
 * of the current binding with respect to an interation (usually, inside a Scan).
 * 
 * @author Romain Vernoux
 * 
 */
@SuppressWarnings("serial")
public class PositionVariable extends RelativeVariable
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(PositionVariable.class);
    
    /**
     * Create an element variable.
     * 
     * @param name
     *            the name of the variable
     */
    public PositionVariable(String name)
    {
        super(name);
    }
    
    /**
     * Create an element variable.
     * 
     * @param name
     *            the name of the variable
     * @param default_alias
     *            the default alias
     */
    public PositionVariable(String name, String default_alias)
    {
        super(name, default_alias);
    }
    
    @Override
    public Term copy()
    {
        return new PositionVariable(this.getName(), getDefaultProjectAlias());
    }
    
}
