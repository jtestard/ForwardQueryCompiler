/**
 * 
 */
package edu.ucsd.forward.query.ast;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;
import edu.ucsd.forward.query.explain.QueryExpressionPrinter;

/**
 * The exists predicate that contains a query node.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class ExistsNode extends AbstractValueExpression
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ExistsNode.class);
    
    /**
     * Constructor.
     * 
     * @param value_expr
     *            the value expression to test.
     * @param location
     *            a location.
     */
    public ExistsNode(ValueExpression value_expr, Location location)
    {
        super(location);
        
        assert (value_expr != null);
        this.addChild(value_expr);
    }
    
    /**
     * Gets the value expression.
     * 
     * @return the value expression.
     */
    public ValueExpression getValueExpression()
    {
        List<AstNode> children = this.getChildren();
        
        if (children.isEmpty()) return null;
        
        assert (children.size() == 1);
        assert (children.get(0) instanceof ValueExpression);
        
        return (ValueExpression) children.get(0);
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(this.getIndent(tabs) + "EXISTS" + QueryExpressionPrinter.LEFT_PAREN + QueryExpressionPrinter.NL);
        
        getValueExpression().toQueryString(sb, tabs + 1, data_source);
        sb.append(QueryExpressionPrinter.NL);
        
        sb.append(this.getIndent(tabs) + QueryExpressionPrinter.RIGHT_PAREN);
        
    }
    
    @Override
    public String toExplainString()
    {
        String out = super.toExplainString();
        
        return out;
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitExistsNode(this);
    }
    
    @Override
    public AstNode copy()
    {
        ExistsNode copy = new ExistsNode((ValueExpression) getValueExpression().copy(), this.getLocation());
        
        super.copy(copy);
        
        return copy;
    }
    
}
