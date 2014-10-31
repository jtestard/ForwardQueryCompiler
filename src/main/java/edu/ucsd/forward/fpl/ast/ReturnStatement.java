/**
 * 
 */
package edu.ucsd.forward.fpl.ast;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.fpl.FplCompilationException;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * The RETURN statement immediately completes the execution of a function and returns control to the invoker.
 * 
 * @author Yupeng
 * 
 */
public class ReturnStatement extends AbstractStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ReturnStatement.class);
    
    /**
     * Constructor.
     * 
     * @param location
     *            a location.
     */
    public ReturnStatement(Location location)
    {
        super(location);
    }
    
    /**
     * Sets the expression to return.
     * 
     * @param expression
     *            the expression to return.
     */
    public void setExpression(ValueExpression expression)
    {
        assert expression != null;
        this.addChild(expression);
    }
    
    /**
     * Gets the expression to return.
     * 
     * @return the expression to return.
     */
    public ValueExpression getExpression()
    {
        List<AstNode> children = this.getChildren();
        
        if (children.isEmpty()) return null;
        
        assert (children.get(0) instanceof ValueExpression);
        
        return (ValueExpression) children.get(0);
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws FplCompilationException
    {
        return visitor.visitReturnStatement(this);
    }
    
    @Override
    public AstNode copy()
    {
        ReturnStatement copy = new ReturnStatement(this.getLocation());
        super.copy(copy);
        copy.setExpression((ValueExpression) getExpression().copy());
        return copy;
    }
    
    @Override
    public void toActionString(StringBuilder sb, int tabs)
    {
        sb.append(this.getIndent(tabs) + "RETURN ");
        if (getExpression() != null) getExpression().toQueryString(sb, 0, null);
    }
}
