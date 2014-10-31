/**
 * 
 */
package edu.ucsd.forward.query.ast;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;

/**
 * The abstract class for aliased AST node.
 * 
 * @author Yupeng
 * 
 */
public abstract class AbstractAliasedAstNode extends AbstractQueryConstruct implements AliasedAstNode
{
    private String              m_alias;
    
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractAliasedAstNode.class);
    
    /**
     * Constructor.
     * 
     * @param location
     *            the location of the term.
     */
    public AbstractAliasedAstNode(Location location)
    {
        super(location);
    }
    
    @Override
    public String getAlias()
    {
        return m_alias;
    }
    
    @Override
    public void setAlias(String alias)
    {
//        assert (alias != null) : "The alias cannot be null";
        m_alias = alias;
    }
    
    @Override
    public boolean isAliased()
    {
        return m_alias != null;
    }
    
}
