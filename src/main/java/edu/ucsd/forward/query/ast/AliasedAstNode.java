/**
 * 
 */
package edu.ucsd.forward.query.ast;

/**
 * An AST node that has alias.
 * 
 * @author Yupeng
 * 
 */
public interface AliasedAstNode extends AstNode
{
    /**
     * Sets the alias.
     * 
     * @param alias
     *            the alias
     */
    public void setAlias(String alias);
    
    /**
     * Gets the alias.
     * 
     * @return the alias
     */
    public String getAlias();
    
    /**
     * Checks if the node has an alias.
     * 
     * @return <code>true</code> if the node contains alias, otherwise <code>false</code>
     */
    public boolean isAliased();
    
}
