package edu.ucsd.forward.query.ast;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * The order by item.
 * 
 * @author Yupeng
 * 
 */
@SuppressWarnings("serial")
public class OrderByItem extends AbstractQueryConstruct
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(OrderByItem.class);
    
    private Spec                m_spec;
    
    private Nulls               m_nulls;
    
    /**
     * The ordering specification of the order by item.
     * 
     * @author Yupeng
     * 
     */
    public enum Spec
    {
        ASC, DESC
    };
    
    /**
     * The nulls ordering of the order by item.
     * 
     * @author Michalis Petropoulos
     * 
     */
    public enum Nulls
    {
        FIRST, LAST
    };
    
    /**
     * Constructor.
     * 
     * @param expression
     *            the expression.
     * @param spec
     *            the ordering specification.
     * @param nulls
     *            the nulls ordering.
     * @param location
     *            a location.
     */
    public OrderByItem(ValueExpression expression, Spec spec, Nulls nulls, Location location)
    {
        super(location);
        
        assert (expression != null);
        this.addChild(expression);
        
        m_spec = (spec == null) ? Spec.ASC : spec;
        m_nulls = (nulls == null) ? Nulls.LAST : nulls;
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
     * Gets the ordering specification.
     * 
     * @return the ordering specification.
     */
    public Spec getSpec()
    {
        return m_spec;
    }
    
    /**
     * Gets the nulls ordering.
     * 
     * @return the nulls ordering.
     */
    public Nulls getNulls()
    {
        return m_nulls;
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        getExpression().toQueryString(sb, tabs, data_source);
        sb.append((m_spec == Spec.DESC) ? " DESC" : " ASC");
        sb.append((m_nulls == Nulls.FIRST) ? " NULLS FIRST" : " NULLS LAST");
        
    }
    
    @Override
    public String toExplainString()
    {
        return super.toExplainString() + " -> " + m_spec.name() + " NULLS " + m_nulls.name();
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitOrderByItem(this);
    }
    
    @Override
    public AstNode copy()
    {
        OrderByItem copy = new OrderByItem((ValueExpression) getExpression().copy(), m_spec, m_nulls, this.getLocation());
        
        super.copy(copy);
        
        return copy;
    }
    
}
