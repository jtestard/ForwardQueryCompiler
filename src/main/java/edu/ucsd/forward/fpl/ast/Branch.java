/**
 * 
 */
package edu.ucsd.forward.fpl.ast;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.fpl.FplCompilationException;
import edu.ucsd.forward.fpl.explain.FplPrinter;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * The branch in the IF statement.
 * 
 * @author Yupeng
 * 
 */
public class Branch extends AbstractFplConstruct
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(Branch.class);
    
    /**
     * Constructs the branch with the condition.
     * 
     * @param condition
     *            the condition of the branch.
     * @param location
     *            a location.
     */
    public Branch(ValueExpression condition, Location location)
    {
        super(location);
        
        assert condition != null;
        this.addChild(condition);
    }
    
    /**
     * Adds a statement to execute in the branch.
     * 
     * @param statement
     *            a statement to execute in the branch.
     */
    public void addStatement(Statement statement)
    {
        assert statement != null;
        this.addChild(statement);
    }
    
    /**
     * Gets the condition of the branch.
     * 
     * @return the condition of the branch.
     */
    public ValueExpression getCondition()
    {
        List<AstNode> children = this.getChildren();
        
        if (children.isEmpty()) return null;
        
        assert (children.get(0) instanceof ValueExpression);
        
        return (ValueExpression) children.get(0);
    }
    
    /**
     * Gets the statements in the branch.
     * 
     * @return the statements in the branch.
     */
    public List<Statement> getStatements()
    {
        List<Statement> children = new ArrayList<Statement>();
        boolean first = true;
        for (AstNode arg : this.getChildren())
        {
            if (first)
            {
                first = false;
                continue;
            }
            children.add((Statement) arg);
        }
        
        return children;
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws FplCompilationException
    {
        return visitor.visitBranch(this);
    }
    
    @Override
    public AstNode copy()
    {
        Branch copy = new Branch(getCondition(), this.getLocation());
        super.copy(copy);
        
        for (Statement statement : getStatements())
        {
            copy.addStatement((Statement) statement.copy());
        }
        return copy;
    }
    
    @Override
    public void toActionString(StringBuilder sb, int tabs)
    {
        getCondition().toQueryString(sb, 0, null);
        sb.append(" THEN" + FplPrinter.NL);
        
        for (Statement statement : getStatements())
        {
            statement.toActionString(sb, tabs + 1);
            sb.append(";" + FplPrinter.NL);
        }
    }
}
