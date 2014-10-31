/**
 * 
 */
package edu.ucsd.forward.query.ast;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;
import edu.ucsd.forward.query.explain.QueryExpressionPrinter;

/**
 * A set operation query.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public class SetOpExpression extends AbstractQueryNode
{
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(SetOpExpression.class);
    
    private SetOpType           m_type;
    
    private SetQuantifier       m_set_quantifier = SetQuantifier.ALL;
    
    /**
     * The types of set operations.
     * 
     * @author Michalis Petropoulos
     * 
     */
    public enum SetOpType
    {
        UNION, EXCEPT, INTERSECT, OUTER_UNION
    };
    
    /**
     * Constructor.
     * 
     * @param type
     *            the type of the set operation.
     * @param set_quantifier
     *            the set quantifier.
     * @param left
     *            the left child query.
     * @param right
     *            the right child query.
     * @param location
     *            a location.
     */
    public SetOpExpression(SetOpType type, SetQuantifier set_quantifier, QueryNode left, QueryNode right, Location location)
    {
        super(location);
        
        assert (type != null);
        assert (set_quantifier != null);
        assert (left != null);
        assert (right != null);
        
        m_type = type;
        this.addChild(left);
        this.addChild(right);
        
        m_set_quantifier = set_quantifier;
    }
    
    /**
     * Gets the type of the set operation.
     * 
     * @return the type of the set operation.
     */
    public SetOpType getSetOpType()
    {
        return m_type;
    }
    
    /**
     * Gets the set quantifier.
     * 
     * @return the set quantifier.
     */
    public SetQuantifier getSetQuantifier()
    {
        return m_set_quantifier;
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(this.getIndent(tabs) + QueryExpressionPrinter.LEFT_PAREN + QueryExpressionPrinter.NL);
        
        ((QueryConstruct) this.getChildren().get(0)).toQueryString(sb, tabs + 1, data_source);
        sb.append(QueryExpressionPrinter.NL);
        
        sb.append(this.getIndent(tabs + 1) + m_type.name() + " " + m_set_quantifier.name() + QueryExpressionPrinter.NL);
        
        ((QueryConstruct) this.getChildren().get(1)).toQueryString(sb, tabs + 1, data_source);
        sb.append(QueryExpressionPrinter.NL);
        
        sb.append(this.getIndent(tabs) + QueryExpressionPrinter.RIGHT_PAREN);
        
    }
    
    @Override
    public String toExplainString()
    {
        return super.toExplainString() + " -> " + m_type.name() + " " + m_set_quantifier.name();
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitSetOpExpression(this);
    }
    
    @Override
    public AstNode copy()
    {
        QueryNode left_copy = (QueryNode) this.getChildren().get(0).copy();
        QueryNode right_copy = (QueryNode) this.getChildren().get(1).copy();
        SetOpExpression copy = new SetOpExpression(m_type, m_set_quantifier, left_copy, right_copy, this.getLocation());
        
        super.copy(copy);
        
        return copy;
    }
    
}
