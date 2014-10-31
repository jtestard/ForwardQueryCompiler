package edu.ucsd.forward.query.ast;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;
import edu.ucsd.forward.query.explain.QueryExpressionPrinter;

/**
 * The join item, which joins two from item with a join type.
 * 
 * @author Yupeng
 * 
 */
public class JoinItem extends AbstractQueryConstruct implements FromItem
{
    @SuppressWarnings("unused")
    private static final Logger log          = Logger.getLogger(JoinItem.class);
    
    private JoinType            m_join_type  = JoinType.INNER;
    
    private boolean             m_is_natural = false;
    
    /**
     * Constructs with the items and join type.
     * 
     * @param left_item
     *            the left join item
     * @param right_item
     *            the right join item
     * @param join_type
     *            the join type
     * @param location
     *            a location.
     */
    public JoinItem(FromItem left_item, FromItem right_item, JoinType join_type, Location location)
    {
        this(left_item, right_item, join_type, false, location);
    }
    
    /**
     * Constructs with the items, join type and natural flag.
     * 
     * @param left_item
     *            the left join item.
     * @param right_item
     *            the right join item.
     * @param join_type
     *            the join type.
     * @param is_natural
     *            determines whether the join is natural.
     * @param location
     *            a location.
     */
    public JoinItem(FromItem left_item, FromItem right_item, JoinType join_type, boolean is_natural, Location location)
    {
        super(location);
        
        assert (join_type != null);
        assert (left_item != null);
        assert (right_item != null);
        
        this.addChild(left_item);
        this.addChild(right_item);
        
        m_join_type = join_type;
        m_is_natural = is_natural;
    }
    
    /**
     * Sets the on condition.
     * 
     * @param on_condition
     *            the on condition
     */
    public void setOnCondition(ValueExpression on_condition)
    {
        assert (on_condition != null);
        
        if (getOnCondition() != null)
        {
            this.removeChild(getOnCondition());
        }
        
        this.addChild(on_condition);
    }
    
    /**
     * Gets the on condition.
     * 
     * @return the on condition
     */
    public ValueExpression getOnCondition()
    {
        List<AstNode> children = this.getChildren();
        
        if (children.size() < 3) return null;
        
        assert (children.get(2) instanceof ValueExpression);
        
        return (ValueExpression) children.get(2);
    }
    
    /**
     * Checks if there is on condition in the join item.
     * 
     * @return <code>true</code> if there is on condition, <code>false</code> otherwise
     */
    public boolean hasOnCondition()
    {
        return getOnCondition() != null;
    }
    
    /**
     * Checks if the join item is natural.
     * 
     * @return <code>true</code> if the join item is natural, <code>false</code> otherwise
     */
    public boolean isNatural()
    {
        return m_is_natural;
    }
    
    /**
     * Gets the left item.
     * 
     * @return the left item
     */
    public FromItem getLeftItem()
    {
        List<AstNode> children = this.getChildren();
        
        if (children.isEmpty()) return null;
        
        assert (children.get(0) instanceof FromItem);
        
        return (FromItem) children.get(0);
    }
    
    /**
     * Gets the right item.
     * 
     * @return the right item
     */
    public FromItem getRightItem()
    {
        List<AstNode> children = this.getChildren();
        
        if (children.size() < 2) return null;
        
        assert (children.get(1) instanceof FromItem);
        
        return (FromItem) children.get(1);
    }
    
    /**
     * Gets the join type.
     * 
     * @return the join type
     */
    public JoinType getJoinType()
    {
        return m_join_type;
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        // sb.append(this.getIndent(tabs) + QueryExpressionPrinter.LEFT_PAREN + QueryExpressionPrinter.NL);
        
        getLeftItem().toQueryString(sb, tabs + 1, data_source);
        sb.append(QueryExpressionPrinter.NL);
        
        sb.append(this.getIndent(tabs + 1) + getJoinType().getName() + " JOIN " + QueryExpressionPrinter.NL);
        
        getRightItem().toQueryString(sb, tabs + 1, data_source);
        sb.append(QueryExpressionPrinter.NL);
        
        if (hasOnCondition())
        {
            sb.append(this.getIndent(tabs + 1) + "ON ");
            getOnCondition().toQueryString(sb, tabs, data_source);
            sb.append(QueryExpressionPrinter.NL);
        }
        
    }
    
    @Override
    public String toExplainString()
    {
        String out = super.toExplainString();
        out += " -> ";
        if (this.m_is_natural) out += " NATURAL ";
        out += m_join_type.getName();
        
        return out;
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitJoinItem(this);
    }
    
    @Override
    public AstNode copy()
    {
        FromItem left_copy = (FromItem) this.getLeftItem().copy();
        FromItem right_copy = (FromItem) this.getRightItem().copy();
        JoinItem copy = new JoinItem(left_copy, right_copy, m_join_type, m_is_natural, this.getLocation());
        
        super.copy(copy);
        
        if (this.hasOnCondition())
        {
            ValueExpression on_condition_copy = (ValueExpression) this.getOnCondition().copy();
            copy.setOnCondition(on_condition_copy);
        }
        
        return copy;
    }
    
}
