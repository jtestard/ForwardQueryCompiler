package edu.ucsd.forward.query.logical.term;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A special type of relative variable that correspond to an element variable (name "tuple variable" in SQL).
 * 
 * @author Romain Vernoux
 * 
 */
@SuppressWarnings("serial")
public class ElementVariable extends RelativeVariable
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ElementVariable.class);
    
    /**
     * Create an element variable.
     * 
     * @param name
     *            the name of the variable
     */
    public ElementVariable(String name)
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
    public ElementVariable(String name, String default_alias)
    {
        super(name, default_alias);
    }
    
    @Override
    public Term copy()
    {
        return new ElementVariable(this.getName(), getDefaultProjectAlias());
    }
    
}
