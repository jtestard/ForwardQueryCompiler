/**
 * 
 */
package edu.ucsd.forward.query.ast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.exception.ExceptionMessages.QueryParsing;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryParsingException;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;
import edu.ucsd.forward.query.explain.QueryExpressionPrinter;

/**
 * A query expression, which can have set operator, order by clause and offset/fetch items.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public class QueryExpression extends AbstractQueryNode
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(QueryExpression.class);
    
    private QueryNode           m_body;
    
    private List<OrderByItem>   m_order_by_items;
    
    private ValueExpression     m_offset;
    
    private ValueExpression     m_fetch;
    
    private List<WithItem>      m_with_items;
    
    /**
     * The default constructor.
     * 
     * @param location
     *            a location.
     */
    public QueryExpression(Location location)
    {
        super(location);
        
        m_order_by_items = new ArrayList<OrderByItem>();
        m_with_items = new ArrayList<WithItem>();
    }
    
    /**
     * Returns the body query.
     * 
     * @return the body query.
     */
    public QueryNode getBody()
    {
        return m_body;
    }
    
    /**
     * Gets the order by items.
     * 
     * @return the order by items.
     */
    public List<OrderByItem> getOrderByItems()
    {
        return Collections.unmodifiableList(m_order_by_items);
    }
    
    /**
     * Gets the with items.
     * 
     * @return the with items.
     */
    public List<WithItem> getWithItems()
    {
        return Collections.unmodifiableList(m_with_items);
    }
    
    /**
     * Gets the offset expression.
     * 
     * @return the offset expression.
     */
    public ValueExpression getOffset()
    {
        return m_offset;
    }
    
    /**
     * Gets the fetch expression.
     * 
     * @return the fetch expression.
     */
    public ValueExpression getFetch()
    {
        return m_fetch;
    }
    
    /**
     * Sets the body query.
     * 
     * @param query
     *            a query.
     * @throws QueryParsingException
     *             when consecutive ORDER BY, OFFSET or FETCH clauses are encountered.
     */
    public void setBody(QueryNode query) throws QueryParsingException
    {
        assert (query != null);
        
        if (m_body != null)
        {
            this.removeChild(m_body);
        }
        
        // Merge consecutive query expression AST nodes, separated by parenthesis in the query string, into a single one.
        if (query instanceof QueryExpression)
        {
            QueryExpression query_expr = (QueryExpression) query;
            
            if (query_expr.hasOrderByClause())
            {
                this.setOrderByItems(query_expr.getOrderByItems());
            }
            
            if (query_expr.getOffset() != null)
            {
                ValueExpression offset = (ValueExpression) query_expr.getOffset().copy();
                this.setOffset(offset);
            }
            
            if (query_expr.getFetch() != null)
            {
                ValueExpression fetch = (ValueExpression) query_expr.getFetch().copy();
                this.setFetch(fetch);
            }
            
            this.setBody(query_expr.getBody());
        }
        else
        {
            m_body = (QueryNode) query.copy();
            this.addChild(m_body);
        }
    }
    
    /**
     * Resets the body query.
     */
    public void resetBody()
    {
        if (m_body != null)
        {
            this.removeChild(m_body);
            m_body = null;
        }
    }
    
    /**
     * Sets the order by items.
     * 
     * @param items
     *            the order by items.
     * @throws QueryParsingException
     *             when consecutive ORDER BY clauses are encountered.
     */
    public void setOrderByItems(List<OrderByItem> items) throws QueryParsingException
    {
        assert (items != null);
        assert (!items.isEmpty());
        
        if (this.hasOrderByClause())
        {
            // Consecutive ORDER BY clauses
            throw new QueryParsingException(QueryParsing.CONSECUTIVE_ORDER_BY_CLAUSES, items.get(0).getExpression().getLocation());
        }
        
        for (OrderByItem item : items)
        {
            OrderByItem copy = (OrderByItem) item.copy();
            m_order_by_items.add(copy);
            this.addChild(copy);
        }
    }
    
    /**
     * Resets the order by items.
     */
    public void resetOrderByItems()
    {
        for (OrderByItem item : m_order_by_items)
        {
            this.removeChild(item);
        }
        m_order_by_items.clear();
    }
    
    /**
     * Sets the with items.
     * 
     * @param items
     *            the with items.
     * 
     */
    public void setWithItems(List<WithItem> items)
    {
        assert (items != null);
        
        for (WithItem item : items)
        {
            WithItem copy = (WithItem) item.copy();
            m_with_items.add(copy);
            this.addChild(copy);
        }
    }
    
    /**
     * Resets the with items.
     */
    public void resetWithItems()
    {
        for (WithItem item : m_with_items)
        {
            removeChild(item);
        }
        m_with_items.clear();
    }
    
    /**
     * Check if the query expression has an order by clause.
     * 
     * @return <code>true</code> if the query expression has an order by clause, otherwise <code>false</code>.
     */
    public boolean hasOrderByClause()
    {
        return !m_order_by_items.isEmpty();
    }
    
    /**
     * Sets the offset expression.
     * 
     * @param offset
     *            the offset expression.
     * @throws QueryParsingException
     *             when consecutive OFFSET clauses are encountered.
     */
    public void setOffset(ValueExpression offset) throws QueryParsingException
    {
        assert (offset != null);
        
        if (m_offset != null)
        {
            // Consecutive OFFSET clauses
            throw new QueryParsingException(QueryParsing.CONSECUTIVE_OFFSET_CLAUSES, offset.getLocation());
        }
        
        if (m_offset != null)
        {
            this.removeChild(m_offset);
        }
        
        m_offset = offset;
        this.addChild(m_offset);
    }
    
    /**
     * Resets the offset expression.
     */
    public void resetOffset()
    {
        if (m_offset != null)
        {
            this.removeChild(m_offset);
        }
    }
    
    /**
     * Sets the fetch expression.
     * 
     * @param fetch
     *            the fetch expression.
     * @throws QueryParsingException
     *             when consecutive FETCH clauses are encountered.
     */
    public void setFetch(ValueExpression fetch) throws QueryParsingException
    {
        assert (fetch != null);
        
        if (m_fetch != null)
        {
            // Consecutive FETCH clauses
            throw new QueryParsingException(QueryParsing.CONSECUTIVE_FETCH_CLAUSES, fetch.getLocation());
        }
        
        if (m_fetch != null)
        {
            this.removeChild(m_fetch);
        }
        
        m_fetch = fetch;
        this.addChild(m_fetch);
    }
    
    /**
     * Resets the fetch expression.
     */
    public void resetFetch()
    {
        if (m_fetch != null)
        {
            this.removeChild(m_fetch);
        }
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        int inner_tabs = tabs;
        AstNode parent = this.getParent();
        if (parent != null)
        {
            inner_tabs++;
            sb.append(this.getIndent(tabs) + QueryExpressionPrinter.LEFT_PAREN + QueryExpressionPrinter.NL);
        }
        
        if (!m_with_items.isEmpty())
        {
            // The WITH items
            sb.append(this.getIndent(inner_tabs) + "WITH ");
            
            Iterator<WithItem> iter = m_with_items.iterator();
            while (iter.hasNext())
            {
                WithItem item = iter.next();
                item.toQueryString(sb, 0, data_source);
                if (iter.hasNext())
                {
                    sb.append(", ");
                }
            }
        }
        
        m_body.toQueryString(sb, inner_tabs, data_source);
        
        if (this.hasOrderByClause())
        {
            sb.append(QueryExpressionPrinter.NL + this.getIndent(inner_tabs) + "ORDER BY ");
        }
        Iterator<OrderByItem> iter = m_order_by_items.iterator();
        while (iter.hasNext())
        {
            OrderByItem item = iter.next();
            item.toQueryString(sb, 0, data_source);
            if (iter.hasNext())
            {
                sb.append(", ");
            }
        }
        
        if (m_offset != null)
        {
            sb.append(QueryExpressionPrinter.NL + this.getIndent(inner_tabs) + "OFFSET ");
            m_offset.toQueryString(sb, 0, data_source);
            sb.append(" ROWS");
        }
        
        if (m_fetch != null)
        {
            sb.append(QueryExpressionPrinter.NL + this.getIndent(inner_tabs) + "FETCH NEXT ");
            m_fetch.toQueryString(sb, 0, data_source);
            sb.append(" ROWS ONLY");
        }
        
        if (parent != null)
        {
            sb.append(QueryExpressionPrinter.NL + this.getIndent(tabs) + QueryExpressionPrinter.RIGHT_PAREN);
        }
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitQueryExpression(this);
    }
    
    @Override
    public AstNode copy()
    {
        QueryExpression copy = new QueryExpression(this.getLocation());
        
        super.copy(copy);
        
        if (m_body != null)
        {
            try
            {
                copy.setBody((QueryNode) m_body.copy());
            }
            catch (QueryParsingException e)
            {
                // This should never happen
                assert (false);
            }
        }
        if (!m_with_items.isEmpty())
        {
            copy.setWithItems(m_with_items);
        }
        if (this.hasOrderByClause())
        {
            try
            {
                copy.setOrderByItems(m_order_by_items);
            }
            catch (QueryParsingException e)
            {
                // This will never happen
                assert (false);
            }
        }
        if (m_offset != null)
        {
            try
            {
                copy.setOffset((ValueExpression) m_offset.copy());
            }
            catch (QueryParsingException e)
            {
                // This will never happen
                assert (false);
            }
        }
        if (m_fetch != null)
        {
            try
            {
                copy.setFetch((ValueExpression) m_fetch.copy());
            }
            catch (QueryParsingException e)
            {
                // This will never happen
                assert (false);
            }
        }
        
        return copy;
    }
    
}
