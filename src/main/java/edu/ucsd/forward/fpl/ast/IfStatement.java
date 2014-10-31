/**
 * 
 */
package edu.ucsd.forward.fpl.ast;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.fpl.FplCompilationException;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * The IF statement executes or skips a sequence of statements, depending on the value of a Boolean expression.
 * 
 * @author Yupeng
 * 
 */
public class IfStatement extends AbstractStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(IfStatement.class);
    
    /**
     * Constructor.
     * 
     * @param location
     *            a location.
     */
    public IfStatement(Location location)
    {
        super(location);
    }
    
    /**
     * Adds a branch.
     * 
     * @param branch
     *            the branch to add.
     */
    public void addBranch(Branch branch)
    {
        assert branch != null;
        this.addChild(branch);
    }
    
    /**
     * Gets the branches.
     * 
     * @return the branches
     */
    public List<Branch> getBranches()
    {
        List<Branch> children = new ArrayList<Branch>();
        for (AstNode arg : this.getChildren())
        {
            children.add((Branch) arg);
        }
        
        return children;
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws FplCompilationException
    {
        return visitor.visitIfStatement(this);
    }
    
    @Override
    public AstNode copy()
    {
        IfStatement copy = new IfStatement(this.getLocation());
        super.copy(copy);
        for (Branch branch : getBranches())
        {
            copy.addBranch((Branch) branch.copy());
        }
        return copy();
    }
    
    @Override
    public void toActionString(StringBuilder sb, int tabs)
    {
        List<Branch> branches = getBranches();
        for (int i = 0; i < branches.size(); i++)
        {
            if (i == 0)
            {
                sb.append(this.getIndent(tabs) + "IF ");
                branches.get(i).toActionString(sb, tabs);
            }
            else
            {
                sb.append(this.getIndent(tabs) + "ELSIF ");
                branches.get(i).toActionString(sb, tabs);
            }
        }
        
        sb.append(this.getIndent(tabs) + "END IF");
        
    }
}
