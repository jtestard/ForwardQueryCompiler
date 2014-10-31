package edu.ucsd.forward.query.ast.function;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * A general function call AST node, which has a name and a list of arguments. The arguments are child AST nodes of the function
 * call AST node.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public class GeneralFunctionNode extends AbstractFunctionNode
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(FunctionNode.class);
    
    /**
     * Constructs the function call with a name.
     * 
     * @param name
     *            the function call name.
     * @param location
     *            a location.
     */
    public GeneralFunctionNode(String name, Location location)
    {
        super(name, location);
    }
    
    /**
     * Constructs the function call given a function class and arguments.
     * 
     * @param name
     *            the function call name.
     * @param arguments
     *            the arguments.
     * @param location
     *            a location.
     */
    public GeneralFunctionNode(String name, List<ValueExpression> arguments, Location location)
    {
        super(name, location);
        
        this.addArguments(arguments);
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitGeneralFunctionNode(this);
    }
    
    @Override
    public AstNode copy()
    {
        GeneralFunctionNode copy = new GeneralFunctionNode(this.getFunctionName(), this.getLocation());
        
        super.copy(copy);
        
        for (ValueExpression expr : getArguments())
        {
            copy.addArgument((ValueExpression) expr.copy());
        }
        
        return copy;
    }
    
}
