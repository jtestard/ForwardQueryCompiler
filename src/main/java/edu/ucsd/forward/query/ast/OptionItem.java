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
 * The switch option item.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public class OptionItem extends AbstractAliasedAstNode
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(OptionItem.class);
    
    /**
     * Constructs the option item with when expression and then expression.
     * 
     * @param when_expr
     *            the when expression.
     * @param then_expr
     *            the then expression.
     * @param location
     *            a location.
     */
    public OptionItem(ValueExpression when_expr, ValueExpression then_expr, Location location)
    {
        super(location);
        
        assert (when_expr != null);
        this.addChild(when_expr);
        
        assert (then_expr != null);
        this.addChild(then_expr);
    }
    
    /**
     * Gets the when expression.
     * 
     * @return the when expression.
     */
    public ValueExpression getWhenExpression()
    {
        List<AstNode> children = this.getChildren();
        
        if (children.isEmpty()) return null;
        
        assert (children.get(0) instanceof ValueExpression);
        
        return (ValueExpression) children.get(0);
    }
    
    /**
     * Gets the then expression.
     * 
     * @return the then expression.
     */
    public ValueExpression getThenExpression()
    {
        List<AstNode> children = this.getChildren();
        
        if (children.size() < 2) return null;
        
        assert (children.get(1) instanceof ValueExpression);
        
        return (ValueExpression) children.get(1);
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(this.getIndent(tabs) + "WHEN" + QueryExpressionPrinter.NL);
        
        getWhenExpression().toQueryString(sb, tabs + 1, data_source);
        sb.append(QueryExpressionPrinter.NL);
        
        sb.append(this.getIndent(tabs) + "THEN" + QueryExpressionPrinter.NL);
        
        getThenExpression().toQueryString(sb, tabs + 1, data_source);
        
        if (this.getAlias() != null)
        {
            sb.append(" AS " + this.getAlias());
        }
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitOptionItem(this);
    }
    
    @Override
    public AstNode copy()
    {
        OptionItem copy = new OptionItem((ValueExpression) getWhenExpression().copy(),
                                         (ValueExpression) getThenExpression().copy(), this.getLocation());
        
        super.copy(copy);
        
        copy.setAlias(this.getAlias());
        
        return copy;
    }
    
}
