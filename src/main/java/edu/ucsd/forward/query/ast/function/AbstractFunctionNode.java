package edu.ucsd.forward.query.ast.function;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.ast.AbstractValueExpression;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.explain.QueryExpressionPrinter;
import edu.ucsd.forward.query.function.Function.Notation;
import edu.ucsd.forward.query.function.FunctionRegistry;
import edu.ucsd.forward.query.function.FunctionRegistryException;
import edu.ucsd.forward.query.function.math.NegFunction;

/**
 * An abstract implementation of the function node interface.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractFunctionNode extends AbstractValueExpression implements FunctionNode
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(FunctionNode.class);
    
    private String              m_func_name;
    
    /**
     * Constructs the function call with a name.
     * 
     * @param name
     *            the function call name.
     * @param location
     *            a location.
     */
    public AbstractFunctionNode(String name, Location location)
    {
        super(location);
        
        assert (name != null);
        m_func_name = name;
    }
    
    @Override
    public String getFunctionName()
    {
        return m_func_name;
    }
    
    @Override
    public List<ValueExpression> getArguments()
    {
        List<ValueExpression> children = new ArrayList<ValueExpression>();
        for (AstNode arg : this.getChildren())
        {
            children.add((ValueExpression) arg);
        }
        
        return children;
    }
    
    /**
     * Adds all arguments in the same order.
     * 
     * @param arguments
     *            the argument to be added.
     */
    public void addArguments(List<ValueExpression> arguments)
    {
        assert (arguments != null);
        for (ValueExpression argument : arguments)
        {
            addArgument(argument);
        }
    }
    
    /**
     * Adds one argument.
     * 
     * @param argument
     *            the argument to be added.
     */
    public void addArgument(ValueExpression argument)
    {
        assert (argument != null);
        this.addChild(argument);
    }
    
    /**
     * Adds one argument at the specified position in this list (optional operation). Shifts the argument currently at that position
     * (if any) and any subsequent elements to the right (adds one to their indices).
     * 
     * @param index
     *            index at which the specified argument is to be inserted.
     * @param argument
     *            the argument to be added.
     */
    public void addArgument(int index, ValueExpression argument)
    {
        assert (argument != null);
        this.addChild(index, argument);
    }
    
    /**
     * Removes all arguments from the function, and sets the parent of all arguments to be null.
     */
    public void removeArguments()
    {
        for (AstNode arg : getArguments())
        {
            this.removeChild(arg);
        }
    }
    
    /**
     * Removes one argument from the function, and sets the parent of the argument to be null.
     * 
     * @param argument
     *            the argument to be removed
     */
    public void removeArgument(ValueExpression argument)
    {
        assert (argument != null);
        this.removeChild(argument);
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(this.getIndent(tabs));
        
        // Special treatment for NEG function because it has the same operator as SUB
        if (m_func_name.equals(NegFunction.NAME))
        {
            sb.append("-" + QueryExpressionPrinter.LEFT_PAREN);
            
            for (ValueExpression argument : getArguments())
            {
                argument.toQueryString(sb, 0, data_source);
                sb.append(", ");
            }
            if (!getArguments().isEmpty())
            {
                sb.delete(sb.length() - 2, sb.length());
            }
            sb.append(QueryExpressionPrinter.RIGHT_PAREN);
            return;
        }
        
        Notation notation = null;
        try
        {
            notation = FunctionRegistry.getInstance().getFunction(m_func_name).getNotation();
        }
        catch (FunctionRegistryException e)
        {
            // FIXME: Cleanup
            // This should happen only for FPL functions
            // We always display the function for debugging purpose.
            sb.append(m_func_name + QueryExpressionPrinter.LEFT_PAREN);
            
            for (ValueExpression argument : getArguments())
            {
                argument.toQueryString(sb, 0, data_source);
                sb.append(", ");
            }
            if (!getArguments().isEmpty())
            {
                sb.delete(sb.length() - 2, sb.length());
            }
            sb.append(QueryExpressionPrinter.RIGHT_PAREN);
            
            // log.info(sb.toString());
            
            return;
        }
        
        // Normal functions...
        switch (notation)
        {
            case PREFIX:
                sb.append(m_func_name + QueryExpressionPrinter.LEFT_PAREN);
                
                for (ValueExpression argument : getArguments())
                {
                    argument.toQueryString(sb, 0, data_source);
                    sb.append(", ");
                }
                if (!getArguments().isEmpty())
                {
                    sb.delete(sb.length() - 2, sb.length());
                }
                sb.append(QueryExpressionPrinter.RIGHT_PAREN);
                
                return;
            case INFIX:
                sb.append(QueryExpressionPrinter.LEFT_PAREN);
                for (ValueExpression argument : getArguments())
                {
                    argument.toQueryString(sb, 0, data_source);
                    sb.append(" " + m_func_name + " ");
                }
                int remove_length = m_func_name.length() + 2;
                sb.delete(sb.length() - remove_length, sb.length());
                sb.append(QueryExpressionPrinter.RIGHT_PAREN);
                
                return;
            case POSTFIX:
                sb.append(QueryExpressionPrinter.LEFT_PAREN + QueryExpressionPrinter.LEFT_PAREN);
                
                for (ValueExpression argument : getArguments())
                {
                    argument.toQueryString(sb, 0, data_source);
                    sb.append(", ");
                }
                if (!getArguments().isEmpty())
                {
                    sb.delete(sb.length() - 2, sb.length());
                }
                sb.append(QueryExpressionPrinter.RIGHT_PAREN + m_func_name + QueryExpressionPrinter.RIGHT_PAREN);
                
                return;
            default:
                throw new AssertionError();
        }
    }
    
    @Override
    public String toExplainString()
    {
        StringBuilder sb = new StringBuilder();
        int tabs = 0;
        sb.append(this.getIndent(tabs));
        
        // Special treatment for NEG function because it has the same operator as SUB
        if (m_func_name.equals(NegFunction.NAME))
        {
            sb.append("-" + QueryExpressionPrinter.LEFT_PAREN);
            
            for (ValueExpression argument : getArguments())
            {
                argument.toExplainString();
                sb.append(", ");
            }
            if (!getArguments().isEmpty())
            {
                sb.delete(sb.length() - 2, sb.length());
            }
            sb.append(QueryExpressionPrinter.RIGHT_PAREN);
            return sb.toString();
        }
        
        Notation notation = null;
        try
        {
            notation = FunctionRegistry.getInstance().getFunction(m_func_name).getNotation();
        }
        catch (FunctionRegistryException e)
        {
            // FIXME: Cleanup
            // This should happen only for FPL functions
            // We always display the function for debugging purpose.
            sb.append(m_func_name + QueryExpressionPrinter.LEFT_PAREN);
            
            for (ValueExpression argument : getArguments())
            {
                argument.toExplainString();
                sb.append(", ");
            }
            if (!getArguments().isEmpty())
            {
                sb.delete(sb.length() - 2, sb.length());
            }
            sb.append(QueryExpressionPrinter.RIGHT_PAREN);
            
            // log.info(sb.toString());
            
            return sb.toString();
        }
        
        // Normal functions...
        switch (notation)
        {
            case PREFIX:
                sb.append(m_func_name + QueryExpressionPrinter.LEFT_PAREN);
                
                for (ValueExpression argument : getArguments())
                {
                    argument.toExplainString();
                    sb.append(", ");
                }
                if (!getArguments().isEmpty())
                {
                    sb.delete(sb.length() - 2, sb.length());
                }
                sb.append(QueryExpressionPrinter.RIGHT_PAREN);
                
                return sb.toString();
            case INFIX:
                sb.append(QueryExpressionPrinter.LEFT_PAREN);
                for (ValueExpression argument : getArguments())
                {
                    argument.toExplainString();
                    sb.append(" " + m_func_name + " ");
                }
                int remove_length = m_func_name.length() + 2;
                sb.delete(sb.length() - remove_length, sb.length());
                sb.append(QueryExpressionPrinter.RIGHT_PAREN);
                
                return sb.toString();
            case POSTFIX:
                sb.append(QueryExpressionPrinter.LEFT_PAREN + QueryExpressionPrinter.LEFT_PAREN);
                
                for (ValueExpression argument : getArguments())
                {
                    argument.toExplainString();
                    sb.append(", ");
                }
                if (!getArguments().isEmpty())
                {
                    sb.delete(sb.length() - 2, sb.length());
                }
                sb.append(QueryExpressionPrinter.RIGHT_PAREN + m_func_name + QueryExpressionPrinter.RIGHT_PAREN);
                
                return sb.toString();
            default:
                throw new AssertionError();
        }
    }
    
}
