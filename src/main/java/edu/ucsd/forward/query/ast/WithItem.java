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
 * The WITH items in query expression.
 * 
 * @author Yupeng Fu
 * 
 */
public class WithItem extends AbstractQueryConstruct
{
    private static final long   serialVersionUID = 1L;
    
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(WithItem.class);
    
    private String              m_query_name;
    
    /**
     * Constructor.
     * 
     * @param query_name
     *            name of the query
     * @param query_expression
     *            the query expression.
     * @param location
     *            a location.
     */
    public WithItem(String query_name, QueryNode query_expression, Location location)
    {
        super(location);
        assert query_expression != null;
        addChild(query_expression);
        assert query_name != null;
        m_query_name = query_name;
    }
    
    /**
     * Gets the query name.
     * 
     * @return the query name
     */
    public String getQueryName()
    {
        return m_query_name;
    }
    
    /**
     * Gets the query expression.
     * 
     * @return the query expression.
     */
    public QueryNode getQueryExpression()
    {
        List<AstNode> children = this.getChildren();
        
        if (children.isEmpty()) return null;
        
        assert (children.size() == 1);
        assert (children.get(0) instanceof QueryNode);
        
        return (QueryNode) children.get(0);
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitWithItem(this);
    }
    
    @Override
    public AstNode copy()
    {
        WithItem copy = new WithItem(m_query_name, (QueryNode) getQueryExpression().copy(), getLocation());
        super.copy(copy);
        return copy;
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(m_query_name).append(" AS ");
        getQueryExpression().toQueryString(sb, tabs, data_source);
        sb.append("\n");
    }
    
    @Override
    public String toExplainString()
    {
        return super.toExplainString() + " -> " + m_query_name;
    }
}
