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
 * A from expression item, which contains an expression.
 * 
 * @author Yupeng
 * 
 */
public class FromExpressionItem extends AbstractAliasedAstNode implements FromItem
{
    
    private String              m_input_order_var;
    
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(FromExpressionItem.class);
    
    /**
     * Constructs with the from expression.
     * 
     * @param expression
     *            the from expression
     * @param location
     *            a location.
     */
    public FromExpressionItem(ValueExpression expression, Location location)
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
    public ValueExpression getFromExpression()
    {
        List<AstNode> children = this.getChildren();
        
        if (children.isEmpty()) return null;
        
        assert (children.size() == 1);
        assert (children.get(0) instanceof ValueExpression);
        
        return (ValueExpression) children.get(0);
    }
    
    /**
     * Gets the input order alias of this from expression item
     * 
     * @return the input order alias or <code>null</code> if this from expression item has no input order alias.
     */
    public String getInputOrderVariable()
    {
        return m_input_order_var;
    }
    
    /**
     * Sets the input order alias of this from expression item
     * 
     * @param alias
     *            the alias
     */
    public void setInputOrderVariable(String variable)
    {
        m_input_order_var = variable;
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        getFromExpression().toQueryString(sb, tabs, data_source);
        
        if(this.getInputOrderVariable() != null)
        {
            sb.append(" AT " + this.getInputOrderVariable());
        }
        
        if (this.getAlias() != null)
        {
            sb.append(" AS " + this.getAlias());
        }
        
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitFromExpressionItem(this);
    }
    
    @Override
    public AstNode copy()
    {
        FromExpressionItem copy = new FromExpressionItem((ValueExpression) getFromExpression().copy(), this.getLocation());
        
        super.copy(copy);
        
        if (this.getAlias() != null)
        {
            copy.setAlias(this.getAlias());
        }
        
        if (this.getInputOrderVariable() != null)
        {
            copy.setInputOrderVariable(this.getInputOrderVariable());
        }
        
        return copy;
    }
    
}
