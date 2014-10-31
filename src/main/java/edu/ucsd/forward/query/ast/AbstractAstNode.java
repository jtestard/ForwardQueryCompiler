/**
 * 
 */
package edu.ucsd.forward.query.ast;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.explain.QueryExpressionPrinter;
import edu.ucsd.forward.util.tree.AbstractTreeNode2;

/**
 * Represents an abstract implementation of the query AST node interface.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractAstNode extends AbstractTreeNode2<AstNode, ValueExpression, AstNode, AstNode, AstNode> implements
        AstNode
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractAstNode.class);
    
    private Location            m_location;
    
    /**
     * Constructor.
     * 
     * @param location
     *            the location of the term.
     */
    public AbstractAstNode(Location location)
    {
        assert (location != null);
        
        m_location = new LocationImpl(location);
    }
    
    @Override
    public Location getLocation()
    {
        return m_location;
    }
    
    @Override
    public void setLocation(Location location)
    {
        m_location = location;
    }
    
    /**
     * Gets the whitespace as indentation to format the SQL++ query string.
     * 
     * @return the indent whitespace.
     */
    protected String getIndent(int tabs)
    {
        String indent = "";
        for (int i = 0; i < tabs; i++)
        {
            indent += QueryExpressionPrinter.TAB;
        }
        
        return indent;
    }
    
    @Override
    public String toExplainString()
    {
        String class_name = this.getClass().getName();
        String str = class_name.substring(class_name.lastIndexOf('.') + 1, class_name.length());
        
        // Add the alias, if any
        if (this instanceof AliasedAstNode) str += " AS " + ((AliasedAstNode) this).getAlias();
        
        return str;
    }
    
    /**
     * Copies common attributes to an AST node copy.
     * 
     * @param copy
     *            an AST node copy.
     */
    protected void copy(AstNode copy)
    {
    }
    
}
