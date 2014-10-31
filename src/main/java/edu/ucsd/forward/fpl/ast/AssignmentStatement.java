/**
 * 
 */
package edu.ucsd.forward.fpl.ast;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.fpl.FplCompilationException;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.AttributeReference;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * The assignment statement is shortcut for the UPDATE statement.
 * 
 * @author Yupeng
 * 
 */
public class AssignmentStatement extends AbstractStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AssignmentStatement.class);
    
    /**
     * Constructs the assignment statement with the target and value expression.
     * 
     * @param target
     *            the target of the assignment
     * @param expression
     *            the expression to evaluate the value of the assignment
     * @param location
     *            a location.
     */
    public AssignmentStatement(AttributeReference target, ValueExpression expression, Location location)
    {
        super(location);
        
        assert target != null;
        this.addChild(target);
        assert expression != null;
        this.addChild(expression);
    }
    
    /**
     * Gets the target.
     * 
     * @return the target
     */
    public AttributeReference getTarget()
    {
        List<AstNode> children = this.getChildren();
        
        if (children.isEmpty()) return null;
        
        assert (children.get(0) instanceof AttributeReference);
        
        return (AttributeReference) children.get(0);
    }
    
    /**
     * Gets the expression of the assignment.
     * 
     * @return the expression of the assignment.
     */
    public ValueExpression getExpression()
    {
        List<AstNode> children = this.getChildren();
        
        if (children.isEmpty()) return null;
        
        assert (children.get(1) instanceof ValueExpression);
        
        return (ValueExpression) children.get(1);
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws FplCompilationException
    {
        return visitor.visitAssignmentStatement(this);
    }
    
    @Override
    public AstNode copy()
    {
        AssignmentStatement copy = new AssignmentStatement((AttributeReference) getTarget().copy(),
                                                           (ValueExpression) getExpression().copy(), this.getLocation());
        super.copy(copy);
        return copy;
    }
    
    @Override
    public void toActionString(StringBuilder sb, int tabs)
    {
        sb.append(getIndent(tabs));
        getTarget().toQueryString(sb, 0, null);
        sb.append(" := ");
        getExpression().toQueryString(sb, 0, null);
    }
}
