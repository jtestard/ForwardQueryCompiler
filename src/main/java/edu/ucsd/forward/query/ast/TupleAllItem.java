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
 * Represents a qualified asterisk in tuple constructor.
 * 
 * @author Yupeng Fu
 * 
 */
public class TupleAllItem extends AbstractQueryConstruct
{
    private static final long   serialVersionUID = 1L;
    
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(TupleAllItem.class);
    
    private static final String ASTERISK         = "*";
    
    /**
     * Constructor for a qualified asterisk.
     * 
     * @param attr_ref
     *            the qualification of the asterisk.
     * @param location
     *            a location.
     */
    public TupleAllItem(AttributeReference attr_ref, Location location)
    {
        super(location);
        
        this.addChild(attr_ref);
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitTupleAllItem(this);
    }
    
    @Override
    public AstNode copy()
    {
        TupleAllItem copy = new TupleAllItem((AttributeReference) this.getChildren().get(0).copy(), this.getLocation());
        super.copy(copy);
        
        return copy;
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(this.getIndent(tabs));
        
        ((QueryConstruct) this.getChildren().get(0)).toQueryString(sb, 0, data_source);
        sb.append(AttributeReference.PATH_SEPARATOR);
        
        sb.append(ASTERISK);
    }
}
