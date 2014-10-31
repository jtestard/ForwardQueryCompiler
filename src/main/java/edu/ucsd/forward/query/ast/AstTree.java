package edu.ucsd.forward.query.ast;

import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.query.explain.ExplanationPrinter;
import edu.ucsd.forward.query.explain.QueryExpressionPrinter;

/**
 * Represents an AST tree rooted at an expression AST node.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class AstTree implements QueryExpressionPrinter, ExplanationPrinter
{
    /**
     * The root node of the AST tree.
     */
    private QueryStatement m_root;
    
    /**
     * Constructs an instance of the AST tree.
     * 
     * @param root
     *            the root node of the AST tree.
     */
    public AstTree(QueryStatement root)
    {
        assert (root != null);
        m_root = root;
    }
    
    /**
     * Returns the root node of the AST tree.
     * 
     * @return the root AST node.
     */
    public QueryStatement getRoot()
    {
        return m_root;
    }
    
    /**
     * Returns the root node of the AST tree.
     * 
     * @return the root AST node.
     */
    public AstTree copy()
    {
        return new AstTree((QueryStatement) m_root.copy());
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        this.getRoot().toQueryString(sb, tabs, data_source);
    }
    
    @Override
    public String toExplainString()
    {
        return this.toExplainString(this.getRoot(), 0);
    }
    
    /**
     * Recursively visits all AST nodes in the logical query plan in a bottom-up fashion.
     * 
     * @param ast_node
     *            the current AST node being visited.
     * @param tabs
     *            the number of indentation tabs.
     * @return the explanation string.
     */
    private String toExplainString(AstNode ast_node, int tabs)
    {
        String str = "";
        
        for (int i = 0; i < tabs; i++)
        {
            str += "    ";
        }
        
        str += ast_node.toExplainString();
        str += "\n";
        
        for (AstNode child : ast_node.getChildren())
        {
            str += this.toExplainString(child, tabs + 1);
        }
        
        return str;
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        toQueryString(sb, 0, null);
        return sb.toString();
    }
    
}
