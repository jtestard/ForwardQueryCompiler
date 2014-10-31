package edu.ucsd.forward.query.ast.function;

import java.util.List;

import edu.ucsd.forward.query.ast.ValueExpression;

/**
 * A function call AST node, which has a name and a list of arguments. The arguments are child AST nodes of the function call AST
 * node.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public interface FunctionNode extends ValueExpression
{
    /**
     * Gets the function call name.
     * 
     * @return the function call name.
     */
    public String getFunctionName();
    
    /**
     * Gets the function call arguments.
     * 
     * @return the function call arguments.
     */
    public List<ValueExpression> getArguments();
    
    /**
     * Adds one argument.
     * 
     * @param argument
     *            the argument to be added.
     */
    public void addArgument(ValueExpression argument);
    
    /**
     * Adds one argument at the specified position in this list (optional operation). Shifts the argument currently at that position
     * (if any) and any subsequent elements to the right (adds one to their indices).
     * 
     * @param index
     *            index at which the specified argument is to be inserted.
     * @param argument
     *            the argument to be added.
     */
    public void addArgument(int index, ValueExpression argument);
    
    /**
     * Removes one argument from the function, and sets the parent of the argument to be null.
     * 
     * @param argument
     *            the argument to be removed
     */
    public void removeArgument(ValueExpression argument);
    
    /**
     * Removes all arguments from the function, and sets the parent of all arguments to be null.
     */
    public void removeArguments();
    
}
