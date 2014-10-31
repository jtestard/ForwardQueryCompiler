/**
 * 
 */
package edu.ucsd.forward.query.ast.literal;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * Represents a null value literal.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class NullLiteral extends AbstractLiteral implements Literal
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(NullLiteral.class);
    
    /**
     * Constructs a null literal.
     * 
     * @param location
     *            a location.
     */
    public NullLiteral(Location location)
    {
        super(NullValue.NULL, location);
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitNullLiteral(this);
    }
    
    @Override
    public AstNode copy()
    {
        NullLiteral copy = new NullLiteral(this.getLocation());
        
        super.copy(copy);
        
        return copy;
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(this.getIndent(tabs) + NullValue.NULL);
    }
    
}
