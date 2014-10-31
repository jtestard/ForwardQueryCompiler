/**
 * 
 */
package edu.ucsd.forward.query.ast;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;
import edu.ucsd.forward.query.explain.QueryExpressionPrinter;

/**
 * The switch constructor, which is used to construct a switch value. The switch constructor has a list of option items.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public class SwitchNode extends AbstractValueExpression
{
    @SuppressWarnings("unused")
    private static final Logger log      = Logger.getLogger(SwitchNode.class);
    
    private boolean             m_inline = false;
    
    /**
     * Checks if the switch node is set as inline.
     * 
     * @return whether the switch node is set as inline.
     */
    public boolean isInline()
    {
        return m_inline;
    }
    
    /**
     * Sets the switch node as inline.
     * 
     * @param inline
     *            inline value.
     */
    public void setInline(boolean inline)
    {
        m_inline = inline;
    }
    
    /**
     * The default constructor.
     * 
     * @param location
     *            a location.
     */
    public SwitchNode(Location location)
    {
        super(location);
    }
    
    /**
     * Gets the option items.
     * 
     * @return the option items.
     */
    public List<OptionItem> getOptionItems()
    {
        List<OptionItem> children = new ArrayList<OptionItem>();
        for (AstNode arg : this.getChildren())
        {
            children.add((OptionItem) arg);
        }
        
        return children;
    }
    
    /**
     * Adds an option item.
     * 
     * @param item
     *            the option item to add.
     */
    public void addOptionItem(OptionItem item)
    {
        assert (item != null);
        this.addChild(item);
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(this.getIndent(tabs));
        
        if (isInline())
        {
            sb.append("INLINE ");
        }
        
        sb.append("SWITCH" + QueryExpressionPrinter.NL);
        
        for (OptionItem option_item : getOptionItems())
        {
            option_item.toQueryString(sb, tabs + 1, data_source);
            sb.append(QueryExpressionPrinter.NL);
        }
        
        sb.append(this.getIndent(tabs) + "END");
        
    }
    
    @Override
    public String toExplainString()
    {
        String out = super.toExplainString();
        
        return out;
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitSwitchNode(this);
    }
    
    @Override
    public AstNode copy()
    {
        SwitchNode copy = new SwitchNode(this.getLocation());
        
        super.copy(copy);
        copy.setInline(m_inline);
        
        for (OptionItem option_item : getOptionItems())
        {
            copy.addOptionItem((OptionItem) option_item.copy());
        }
        
        return copy;
    }
    
}
