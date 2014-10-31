/**
 * 
 */
package edu.ucsd.forward.query.ast;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * The select all item.
 * 
 * @author Yupeng
 * 
 */
public class SelectAllItem extends AbstractQueryConstruct implements SelectItem
{
    private static final String ASTERISK = "*";
    
    @SuppressWarnings("unused")
    private static final Logger log      = Logger.getLogger(SelectAllItem.class);
    
    /**
     * Default constructor.
     * 
     * @param location
     *            a location.
     */
    public SelectAllItem(Location location)
    {
        super(location);
    }
    
    /**
     * Constructor for a qualified asterisk.
     * 
     * @param attr_ref
     *            the qualification of the asterisk.
     * @param location
     *            a location.
     */
    public SelectAllItem(AttributeReference attr_ref, Location location)
    {
        this(location);
        
        this.addChild(attr_ref);
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(this.getIndent(tabs));
        
        if (this.getChildren().size() > 0)
        {
            ((QueryConstruct) this.getChildren().get(0)).toQueryString(sb, 0, data_source);
            sb.append(AttributeReference.PATH_SEPARATOR);
        }
        
        sb.append(ASTERISK);
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitSelectAllItem(this);
    }
    
    @Override
    public AstNode copy()
    {
        SelectAllItem copy = new SelectAllItem(this.getLocation());
        
        super.copy(copy);
        
        if (this.getChildren().size() > 0)
        {
            copy.addChild(this.getChildren().get(0).copy());
        }
        
        return copy;
    }
    
}
