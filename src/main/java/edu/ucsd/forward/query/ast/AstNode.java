/**
 * 
 */
package edu.ucsd.forward.query.ast;

import edu.ucsd.forward.exception.LocationSupport;
import edu.ucsd.forward.query.explain.ExplanationPrinter;
import edu.ucsd.forward.util.tree.TreeNode2;

/**
 * Represents a node in the query abstract syntax tree (AST).
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public interface AstNode extends TreeNode2<AstNode, ValueExpression, AstNode, AstNode, AstNode>, LocationSupport,
        ExplanationPrinter
{
    /**
     * Creates a copy of the AST node.
     * 
     * @return a copied AST node.
     */
    public AstNode copy();
    
}
