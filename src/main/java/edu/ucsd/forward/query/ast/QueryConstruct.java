/**
 * 
 */
package edu.ucsd.forward.query.ast;

import edu.ucsd.forward.fpl.ast.FplConstruct;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;
import edu.ucsd.forward.query.explain.QueryExpressionPrinter;

/**
 * Represents the constructs in query language.
 * 
 * @author Yupeng
 * 
 */
public interface QueryConstruct extends FplConstruct, QueryExpressionPrinter
{
    /**
     * Asking an AST node to accept a visitor.
     * 
     * @param visitor
     *            the visitor to accept.
     * @return an AST node.
     * @throws QueryCompilationException
     *             when there are more than one suffix matches.
     */
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException;
}
