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
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.ExceptionMessages.QueryParsing;
import edu.ucsd.forward.query.QueryParsingException;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * A window clause.
 * 
 * @author Yupeng Fu
 * 
 */
public class WindowItem extends AbstractQueryConstruct
{
    /**
     * 
     */
    private static final long   serialVersionUID = 1L;
    
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(WindowItem.class);
    
    private List<GroupByItem>   m_partition_by_items;
    
    private List<OrderByItem>   m_order_by_items;
    
    private String              m_rank_alias;
    
    /**
     * The default constructor.
     * 
     * @param location
     *            a location.
     */
    public WindowItem(Location location)
    {
        super(location);
        
        m_partition_by_items = new ArrayList<GroupByItem>();
        m_order_by_items = new ArrayList<OrderByItem>();
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor)
    {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Adds partition by item.
     * 
     * @param item
     *            the group by item.
     */
    public void addPartitionByItem(GroupByItem item)
    {
        assert (item != null);
        
        m_partition_by_items.add(item);
        this.addChild(item);
    }
    
    /**
     * Gets all the group by items.
     * 
     * @return the group by items.
     */
    public List<GroupByItem> getGroupByItems()
    {
        return Collections.unmodifiableList(m_partition_by_items);
    }
    
    public List<OrderByItem> getOrderByItems()
    {
        return Collections.unmodifiableList(m_order_by_items);
    }
    
    public void setRankAlias(String alias)
    {
        m_rank_alias = alias;
    }
    
    public String getRankAlias()
    {
        return m_rank_alias;
    }
    
    @Override
    public AstNode copy()
    {
        WindowItem copy = new WindowItem(getLocation());
        super.copy(copy);
        if (!m_order_by_items.isEmpty())
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
        for (GroupByItem item : getGroupByItems())
        {
            copy.addPartitionByItem((GroupByItem) item.copy());
        }
        copy.setRankAlias(getRankAlias());
        return copy;
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
        
        if (!m_order_by_items.isEmpty())
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
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        // PARTITION BY clause
        if (!m_partition_by_items.isEmpty())
        {
            sb.append("PARTITION BY ");
        }
        Iterator<GroupByItem> group_iter = m_partition_by_items.iterator();
        while (group_iter.hasNext())
        {
            GroupByItem item = group_iter.next();
            item.toQueryString(sb, 0, data_source);
            if (group_iter.hasNext())
            {
                sb.append(", ");
            }
        }
        
        if (!m_order_by_items.isEmpty())
        {
            sb.append(" ORDER BY ");
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
    }
}
