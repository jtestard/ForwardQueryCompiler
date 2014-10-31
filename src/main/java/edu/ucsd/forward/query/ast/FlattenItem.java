package edu.ucsd.forward.query.ast;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * The flatten item, which flattens a collections specified by the right side and found in the left side.
 * 
 * @author Romain
 * 
 */
@SuppressWarnings("serial")
public class FlattenItem extends AbstractQueryConstruct implements FromItem
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(FlattenItem.class);
    
    private JoinType            m_flatten_type;
    
    /**
     * Constructs with the items and flatten type.
     * 
     * @param left_item
     *            the left flatten item
     * @param right_item
     *            the right flatten item
     * @param flatten_type
     *            the flatten type, either inner or outer
     * @param location
     *            a location.
     */
    public FlattenItem(FromItem left_item, FromItem right_item, JoinType flatten_type, Location location)
    {
        super(location);
        
        assert (flatten_type != null);
        assert (left_item != null);
        assert (right_item != null);
        
        assert (right_item instanceof FromExpressionItem);
        FromExpressionItem right = (FromExpressionItem) right_item;
        assert(right.getFromExpression() instanceof AttributeReference);
        
        
        this.addChild(left_item);
        this.addChild(right_item);
        
        assert (flatten_type == JoinType.INNER || flatten_type == JoinType.LEFT_OUTER);
        
        m_flatten_type = flatten_type;
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
    public FromExpressionItem getRightItem()
    {
        List<AstNode> children = this.getChildren();
        
        if (children.size() < 2) return null;
        
        assert (children.get(1) instanceof FromExpressionItem);
        
        return (FromExpressionItem) children.get(1);
    }
    
    /**
     * Gets the flatten type.
     * 
     * @return the flatten type
     */
    public JoinType getFlattenType()
    {
        return m_flatten_type;
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        // sb.append(this.getIndent(tabs) + QueryExpressionPrinter.LEFT_PAREN + QueryExpressionPrinter.NL);
        
        sb.append(getFlattenType() == JoinType.LEFT_OUTER? "OUTER" : "INNER");
        sb.append(" FLATTEN(");
        
        getLeftItem().toQueryString(sb, tabs, data_source);
        sb.append(", ");
        
        getRightItem().toQueryString(sb, tabs, data_source);        
        sb.append(")");        
    }
    
    @Override
    public String toExplainString()
    {
        String out = super.toExplainString();
        
        out += " -> ";
        out += m_flatten_type.getName();
        
        return out;
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitFlattenItem(this);
    }
    
    @Override
    public AstNode copy()
    {
        FromItem left_copy = (FromItem) this.getLeftItem().copy();
        FromItem right_copy = (FromItem) this.getRightItem().copy();
        FlattenItem copy = new FlattenItem(left_copy, right_copy, m_flatten_type, this.getLocation());
        
        super.copy(copy);
        
        return copy;
    }
    
}
