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
 * A host parameter that starts with colon ':', and contains letters, digits, periods '.' and underscores '_'.
 * 
 * @author Yupeng
 * 
 */
public class DynamicParameter extends AbstractValueExpression
{
    @SuppressWarnings("unused")
    private static final Logger log    = Logger.getLogger(DynamicParameter.class);
    
    public static final String  SYMBOL = "?";
    
    /**
     * Default constructor.
     * 
     * @param location
     *            a location.
     */
    public DynamicParameter(Location location)
    {
        super(location);
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitParameter(this);
    }
    
    @Override
    public AstNode copy()
    {
        DynamicParameter copy = new DynamicParameter(this.getLocation());
        
        super.copy(copy);
        
        return copy;
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(SYMBOL);
    }
}
