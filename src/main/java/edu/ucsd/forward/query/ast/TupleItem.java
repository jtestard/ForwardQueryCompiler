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
 * A tuple item, which represents an item of a tuple constructor in a query.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class TupleItem extends AbstractAliasedAstNode
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TupleItem.class);
    
    /**
     * Constructor.
     * 
     * @param expression
     *            the expression.
     * @param alias
     *            the alias.
     * @param location
     *            a location.
     */
    public TupleItem(ValueExpression expression, String alias, Location location)
    {
        super(location);
        
        assert (expression != null);
        this.addChild(expression);
        
        assert (alias != null);
        this.setAlias(alias);
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
        return visitor.visitTupleItem(this);
    }
    
    @Override
    public AstNode copy()
    {
        TupleItem copy = new TupleItem((ValueExpression) getExpression().copy(), this.getAlias(), this.getLocation());
        
        super.copy(copy);
        
        return copy;
    }
    
}
