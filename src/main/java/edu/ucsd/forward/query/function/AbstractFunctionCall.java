/**
 * 
 */
package edu.ucsd.forward.query.function;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.query.explain.QueryExpressionPrinter;
import edu.ucsd.forward.query.function.Function.Notation;
import edu.ucsd.forward.query.function.math.NegFunction;
import edu.ucsd.forward.query.logical.term.AbstractTerm;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;

/**
 * An abstract implementation of the function call interface.
 * 
 * @param <T>
 *            the class of the function definition.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public abstract class AbstractFunctionCall<T extends Function> extends AbstractTerm implements FunctionCall<T>
{
    private String            m_function_name;
    
    private List<Term>        m_arguments;
    
    private FunctionSignature m_signature;
    
    /**
     * Constructor.
     */
    public AbstractFunctionCall()
    {
        m_arguments = new ArrayList<Term>();
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public T getFunction()
    {
        try
        {
            return (T) FunctionRegistry.getInstance().getFunction(m_function_name);
        }
        catch (FunctionRegistryException e)
        {
            // This should not happen
            throw new AssertionError(e);
        }
    }
    
    /**
     * Sets the function.
     * 
     * @param function
     *            the function to set.
     */
    protected void setFunction(T function)
    {
        assert (function != null);
        
        m_function_name = function.getName();
    }
    
    @Override
    public FunctionSignature getFunctionSignature()
    {
        return m_signature;
    }
    
    /**
     * Sets the function signature.
     * 
     * @param signature
     *            the function signature to set.
     */
    protected void setFunctionSignature(FunctionSignature signature)
    {
        assert (signature != null);
        
        m_signature = signature;
        
        for (FunctionSignature sig : this.getFunction().getFunctionSignatures())
        {
            if (sig.getName().equals(signature.getName())) return;
        }
        
        assert (false);
    }
    
    @Override
    public List<Term> getArguments()
    {
        return m_arguments;
    }
    
    @Override
    public void addArgument(Term arg)
    {
        assert (arg != null);
        
        m_arguments.add(arg);
    }
    
    @Override
    public void addArgument(int index, Term arg)
    {
        assert (arg != null);
        assert (index >= 0 && index <= m_arguments.size());
        
        m_arguments.add(index, arg);
    }
    
    @Override
    public int removeArgument(Term arg)
    {
        int index = m_arguments.indexOf(arg);
        
        assert (m_arguments.remove(arg));
        
        return index;
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        List<Variable> result = new ArrayList<Variable>();
        
        for (Term argument : this.getArguments())
        {
            result.addAll(argument.getVariablesUsed());
        }
        
        return result;
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        List<Parameter> result = new ArrayList<Parameter>();
        
        for (Term argument : this.getArguments())
        {
            result.addAll(argument.getFreeParametersUsed());
        }
        
        return result;
    }
    
    @Override
    public List<Parameter> getBoundParametersUsed()
    {
        return Collections.emptyList();
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        // The list of compliant data sources is empty.
        // NOTE: Internal functions have an empty list of compliant data sources.
        switch (metadata.getDataModel())
        {
            case SQLPLUSPLUS:
                // Always compliant with SQL++ data sources
                return true;
            case RELATIONAL:
                // Not always compliant with relational data sources
                // Get the SQL compliance of the function definition.
                boolean compliant = this.getFunction().isSqlCompliant();
                if (!compliant) return false;
                
                // Check that all arguments are SQL compliant
                for (Term argument : this.getArguments())
                {
                    compliant = argument.isDataSourceCompliant(metadata);
                    if (!compliant) return false;
                }
                
                return true;
            default:
                throw new AssertionError();
        }
    }
    
    @Override
    public String getDefaultProjectAlias()
    {
        String out = this.getFunction().getName().toLowerCase() + "_call";
        
        if (!TypeUtil.isValidAttributeName(out))
        {
            out = "func_call";
        }
        
        return out;
    }
    
    @Override
    public String toString()
    {
        return toExplainString();
    }
    
    @Override
    public String toExplainString()
    {
        StringBuilder sb = new StringBuilder();
        String func_name = getFunction().getName();
        // Special treatment for NEG function because it has the same operator as SUB
        if (getFunction().getName().equals(NegFunction.NAME))
        {
            sb.append("-" + QueryExpressionPrinter.LEFT_PAREN);
            
            for (Term argument : getArguments())
            {
                sb.append(argument.toExplainString());
                sb.append(", ");
            }
            if (!getArguments().isEmpty())
            {
                sb.delete(sb.length() - 2, sb.length());
            }
            sb.append(QueryExpressionPrinter.RIGHT_PAREN);
            return sb.toString();
        }
        
        Notation notation = getFunction().getNotation();
        
        // Normal functions...
        switch (notation)
        {
            case PREFIX:
                sb.append(func_name + QueryExpressionPrinter.LEFT_PAREN);
                
                for (Term argument : getArguments())
                {
                    sb.append(argument.toExplainString());
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
                for (Term argument : getArguments())
                {
                    sb.append(argument.toExplainString());
                    sb.append(" " + func_name + " ");
                }
                int remove_length = func_name.length() + 2;
                sb.delete(sb.length() - remove_length, sb.length());
                sb.append(QueryExpressionPrinter.RIGHT_PAREN);
                
                return sb.toString();
            case POSTFIX:
                sb.append(QueryExpressionPrinter.LEFT_PAREN + QueryExpressionPrinter.LEFT_PAREN);
                
                for (Term argument : getArguments())
                {
                    sb.append(argument.toExplainString());
                    sb.append(", ");
                }
                if (!getArguments().isEmpty())
                {
                    sb.delete(sb.length() - 2, sb.length());
                }
                sb.append(QueryExpressionPrinter.RIGHT_PAREN + func_name + QueryExpressionPrinter.RIGHT_PAREN);
                
                return sb.toString();
            default:
                throw new AssertionError();
        }
    }
}
