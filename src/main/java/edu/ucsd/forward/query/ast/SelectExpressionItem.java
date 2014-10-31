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

/**
 * The select expression item, which contains an expression.
 * 
 * @author Yupeng
 * 
 */
public class SelectExpressionItem extends AbstractAliasedAstNode implements SelectItem
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(SelectExpressionItem.class);
    
    /**
     * Constructs with the expression.
     * 
     * @param expression
     *            the expression
     * @param location
     *            a location.
     */
    public SelectExpressionItem(ValueExpression expression, Location location)
    {
        super(location);
        
        assert (expression != null);
        this.addChild(expression);
    }
    
    /**
     * Gets the expression.
     * 
     * @return the expression
     */
    public ValueExpression getExpression()
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
        getExpression().toQueryString(sb, tabs, data_source);
        
        if (this.getAlias() != null)
        {
            sb.append(" AS " + this.getAlias());
        }
        
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitSelectExpressionItem(this);
    }
    
    @Override
    public AstNode copy()
    {
        SelectExpressionItem copy = new SelectExpressionItem((ValueExpression) getExpression().copy(), this.getLocation());
        
        super.copy(copy);
        
        if (this.getAlias() != null)
        {
            copy.setAlias(this.getAlias());
        }
        
        return copy;
    }
    
}
