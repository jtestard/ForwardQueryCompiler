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
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.function.GeneralFunctionNode;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;
import edu.ucsd.forward.query.explain.QueryExpressionPrinter;
import edu.ucsd.forward.query.function.logical.AndFunction;

/**
 * A query specification, which does not have set operator and order by clause.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class QuerySpecification extends AbstractQueryNode
{
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(QuerySpecification.class);
    
    private SetQuantifier       m_set_quantifier = SetQuantifier.ALL;
    
    private List<SelectItem>    m_select_items;
    
    private List<FromItem>      m_from_items;
    
    private ValueExpression     m_where_expr;
    
    private List<GroupByItem>   m_group_by_items;
    
    private ValueExpression     m_having_expr;
    
    private WindowItem          m_window_item;
    
    public enum SelectQualifier
    {
        TUPLE, ELEMENT
    }
    
    private SelectQualifier m_select_qualifier = SelectQualifier.TUPLE;
    
    /**
     * The default constructor.
     * 
     * @param location
     *            a location.
     */
    public QuerySpecification(Location location)
    {
        super(location);
        
        m_select_items = new ArrayList<SelectItem>();
        m_from_items = new ArrayList<FromItem>();
        m_group_by_items = new ArrayList<GroupByItem>();
    }
    
    /**
     * Returns the set quantifier.
     * 
     * @return the set quantifier.
     */
    public SetQuantifier getSetQuantifier()
    {
        return m_set_quantifier;
    }
    
    /**
     * Gets the select items.
     * 
     * @return the select items.
     */
    public List<SelectItem> getSelectItems()
    {
        return Collections.unmodifiableList(m_select_items);
    }
    
    /**
     * Gets the from items.
     * 
     * @return the from items.
     */
    public List<FromItem> getFromItems()
    {
        return Collections.unmodifiableList(m_from_items);
    }
    
    /**
     * Gets the where expression.
     * 
     * @return the where expression.
     */
    public ValueExpression getWhereExpression()
    {
        return m_where_expr;
    }
    
    /**
     * Sets the set quantifier.
     * 
     * @param set_quantifier
     *            a set quantifier.
     */
    public void setSetQuantifier(SetQuantifier set_quantifier)
    {
        assert (set_quantifier != null);
        
        m_set_quantifier = set_quantifier;
    }
    
    /**
     * Sets the select items.
     * 
     * @param items
     *            the select items.
     */
    public void setSelectItems(List<SelectItem> items)
    {
        assert (items != null);
        
        for (SelectItem item : m_select_items)
        {
            this.removeChild(item);
        }
        
        m_select_items = items;
        for (SelectItem item : m_select_items)
        {
            this.addChild(item);
        }
    }
    
    /**
     * Sets the from items.
     * 
     * @param items
     *            the from items.
     */
    public void setFromItems(List<FromItem> items)
    {
        assert (items != null);
        
        for (FromItem item : m_from_items)
        {
            this.removeChild(item);
        }
        
        m_from_items = items;
        for (FromItem item : items)
        {
            this.addChild(item);
        }
    }
    
    /**
     * Sets the where expression.
     * 
     * @param expr
     *            the where expression.
     */
    public void setWhereExpression(ValueExpression expr)
    {
        assert (expr != null);
        
        this.removeWhereExpression();
        
        m_where_expr = expr;
        this.addChild(m_where_expr);
    }
    
    /**
     * Adds one select item.
     * 
     * @param item
     *            the select item to add.
     */
    public void addSelectItem(SelectItem item)
    {
        assert (item != null);
        
        m_select_items.add(item);
        this.addChild(item);
    }
    
    /**
     * Removes one select item.
     * 
     * @param item
     *            the select item to removed.
     */
    public void removeSelectItem(SelectItem item)
    {
        assert (item != null);
        
        m_select_items.remove(item);
        this.removeChild(item);
    }
    
    /**
     * Removes the FROM item as the position in the list of FROM items.
     * 
     * @param index
     *            the index of the FROM item to be removed
     * @return the FROM item previously at the specified position
     */
    public FromItem removeFromItem(int index)
    {
        assert m_from_items.size() > index;
        FromItem item = m_from_items.remove(index);
        this.removeChild(item);
        return item;
    }
    
    /**
     * Adds one from item.
     * 
     * @param item
     *            the from item.
     */
    public void addFromItem(FromItem item)
    {
        assert (item != null);
        
        m_from_items.add(item);
        this.addChild(item);
    }
    
    public void setWindowItem(WindowItem item)
    {
        assert item != null;
        m_window_item = item;
    }
    
    public WindowItem getWindowItem()
    {
        return m_window_item;
    }
    
    /**
     * Adds a condition to the where clause of the query.
     * 
     * @param cond
     *            the condition to be added.
     */
    public void addWhereCondition(ValueExpression cond)
    {
        if (hasWhereExpression())
        {
            // Remove where condition
            ValueExpression expr = removeWhereExpression();
            GeneralFunctionNode fn = new GeneralFunctionNode(AndFunction.NAME, expr.getLocation());
            fn.addArgument(expr);
            fn.addArgument(cond);
            setWhereExpression(fn);
        }
        else
        {
            setWhereExpression(cond);
        }
    }
    
    /**
     * Check if the query specification has a where clause.
     * 
     * @return <code>true</code> if the query specification has a where clause, otherwise <code>false</code>.
     */
    public boolean hasWhereExpression()
    {
        return m_where_expr != null;
    }
    
    /**
     * Removes the where expression.
     * 
     * @return the removed where expression.
     */
    public ValueExpression removeWhereExpression()
    {
        ValueExpression expr = m_where_expr;
        if (this.hasWhereExpression())
        {
            this.removeChild(m_where_expr);
        }
        m_where_expr = null;
        
        return expr;
    }
    
    /**
     * Adds group by item.
     * 
     * @param item
     *            the group by item.
     */
    public void addGroupByItem(GroupByItem item)
    {
        assert (item != null);
        
        m_group_by_items.add(item);
        this.addChild(item);
    }
    
    /**
     * Gets all the group by items.
     * 
     * @return the group by items.
     */
    public List<GroupByItem> getGroupByItems()
    {
        return Collections.unmodifiableList(m_group_by_items);
    }
    
    /**
     * Sets the having expression.
     * 
     * @param expr
     *            the having expression.
     */
    public void setHavingExpression(ValueExpression expr)
    {
        assert (expr != null);
        m_having_expr = expr;
        this.addChild(expr);
    }
    
    /**
     * Gets the having condition.
     * 
     * @return the having expression.
     */
    public ValueExpression getHavingExpression()
    {
        return m_having_expr;
    }
    
    /**
     * Sets the qualifier that specifies if the query contains SELECT ELEMENT or SELECT (TUPLE).
     * 
     * @param qualifier
     *            the qualifier to set
     */
    public void setSelectQualifier(SelectQualifier qualifier)
    {
        m_select_qualifier = qualifier;
    }
    
    /**
     * Tells if the query contains SELECT ELEMENT or SELECT (TUPLE).
     * 
     * @return the select qualifier of this query specificaion
     */
    public SelectQualifier getSelectQualifier()
    {
        return m_select_qualifier;
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        int inner_tabs = tabs;
        
        AstNode parent = this.getParent();
        if (parent != null && !(parent instanceof QueryExpression))
        {
            inner_tabs++;
            sb.append(this.getIndent(tabs) + QueryExpressionPrinter.LEFT_PAREN + QueryExpressionPrinter.NL);
        }
        
        // SELECT clause
        sb.append(this.getIndent(inner_tabs) + "SELECT " + m_set_quantifier.name());
        Iterator<SelectItem> select_iter = m_select_items.iterator();
        while (select_iter.hasNext())
        {
            SelectItem item = select_iter.next();
            sb.append(QueryExpressionPrinter.NL);
            item.toQueryString(sb, inner_tabs + 1, data_source);
            if (select_iter.hasNext())
            {
                sb.append(", ");
            }
        }
        
        // Window clause
        if (m_window_item != null)
        {
            sb.append(", row_number() OVER(");
            m_window_item.toQueryString(sb, inner_tabs, data_source);
            sb.append(") AS ");
            if (m_window_item.getRankAlias() != null)
            {
                sb.append(m_window_item.getRankAlias());
            }
        }
        
        // FROM clause
        if (!m_from_items.isEmpty())
        {
            sb.append(QueryExpressionPrinter.NL + this.getIndent(inner_tabs) + "FROM");
        }
        Iterator<FromItem> from_iter = m_from_items.iterator();
        while (from_iter.hasNext())
        {
            FromItem item = from_iter.next();
            sb.append(QueryExpressionPrinter.NL);
            item.toQueryString(sb, inner_tabs + 1, data_source);
            if (from_iter.hasNext())
            {
                sb.append(", ");
            }
        }
        
        // WHERE clause
        if (this.hasWhereExpression())
        {
            sb.append(QueryExpressionPrinter.NL + this.getIndent(inner_tabs) + "WHERE");
            sb.append(QueryExpressionPrinter.NL);
            m_where_expr.toQueryString(sb, inner_tabs + 1, data_source);
        }
        
        // GROUP BY clause
        if (!m_group_by_items.isEmpty())
        {
            sb.append(QueryExpressionPrinter.NL + this.getIndent(inner_tabs) + "GROUP BY ");
        }
        Iterator<GroupByItem> group_iter = m_group_by_items.iterator();
        while (group_iter.hasNext())
        {
            GroupByItem item = group_iter.next();
            item.toQueryString(sb, 0, data_source);
            if (group_iter.hasNext())
            {
                sb.append(", ");
            }
        }
        
        // HAVING clause
        if (getHavingExpression() != null)
        {
            sb.append(QueryExpressionPrinter.NL + this.getIndent(inner_tabs) + "HAVING");
            sb.append(QueryExpressionPrinter.NL);
            m_having_expr.toQueryString(sb, inner_tabs + 1, data_source);
        }
        
        if (parent != null && !(parent instanceof QueryExpression))
        {
            sb.append(QueryExpressionPrinter.NL + this.getIndent(tabs) + QueryExpressionPrinter.RIGHT_PAREN);
        }
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitQuerySpecification(this);
    }
    
    @Override
    public AstNode copy()
    {
        QuerySpecification copy = new QuerySpecification(this.getLocation());
        
        super.copy(copy);
        
        copy.setSetQuantifier(m_set_quantifier);
        for (SelectItem item : m_select_items)
            copy.addSelectItem((SelectItem) item.copy());
        for (FromItem item : m_from_items)
            copy.addFromItem((FromItem) item.copy());
        if (m_where_expr != null)
        {
            copy.setWhereExpression((ValueExpression) m_where_expr.copy());
        }
        for (GroupByItem item : m_group_by_items)
            copy.addGroupByItem((GroupByItem) item.copy());
        if (m_having_expr != null)
        {
            copy.setHavingExpression((ValueExpression) m_having_expr.copy());
        }
        if (m_window_item != null)
        {
            copy.setWindowItem((WindowItem) m_window_item.copy());
        }
        copy.setSelectQualifier(m_select_qualifier);
        
        return copy;
    }
    
}
