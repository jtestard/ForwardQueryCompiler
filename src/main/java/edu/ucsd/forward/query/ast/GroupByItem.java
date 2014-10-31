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
 * A group by item, which represents an item in the group by clause of a query.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public class GroupByItem extends AbstractQueryConstruct
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(GroupByItem.class);
    
    /**
     * Constructor.
     * 
     * @param location
     *            a location.
     */
    public GroupByItem(Location location)
    {
        super(location);
    }
    
    /**
     * Sets the expression.
     * 
     * @param expr
     *            the expression.
     */
    public void setExpression(ValueExpression expr)
    {
        assert (expr != null);
        List<AstNode> children = this.getChildren();
        if (!children.isEmpty())
        {
            removeChild(getExpression());
        }
        this.addChild(expr);
    }
    
    /**
     * Gets the expression.
     * 
     * @return the expression.
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
        this.getExpression().toQueryString(sb, tabs, data_source);
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitGroupByItem(this);
    }
    
    @Override
    public AstNode copy()
    {
        GroupByItem copy = new GroupByItem(this.getLocation());
        
        super.copy(copy);
        
        copy.setExpression((ValueExpression) this.getExpression().copy());
        
        return copy;
    }
    
}
