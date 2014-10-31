/**
 * 
 */
package edu.ucsd.forward.fpl.ast;

import edu.ucsd.forward.fpl.FplCompilationException;
import edu.ucsd.forward.fpl.explain.FplPrinter;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * Represents a construct in the action language.
 * 
 * @author Yupeng
 * 
 */
public interface FplConstruct extends AstNode, FplPrinter
{
    /**
     * Asking an AST node to accept a visitor.
     * 
     * @param visitor
     *            the visitor to accept.
     * @return an AST node.
     * @throws FplCompilationException
     *             when there are more than one suffix matches.
     */
    public Object accept(AstNodeVisitor visitor) throws FplCompilationException;
}
