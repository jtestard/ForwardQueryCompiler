package edu.ucsd.forward.query.function.conditional;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.type.NullType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.UnknownType;
import edu.ucsd.forward.exception.ExceptionMessages.QueryCompilation;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.function.AbstractFunctionCall;
import edu.ucsd.forward.query.function.FunctionRegistry;
import edu.ucsd.forward.query.function.FunctionRegistryException;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.term.Term;

/**
 * The case function call.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class CaseFunctionCall extends AbstractFunctionCall<CaseFunction>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(CaseFunctionCall.class);
    
    /**
     * Constructs the case function call for a given list of arguments. The constructor copies the arguments into an internal list.
     * 
     * @param arguments
     *            the function arguments.
     */
    public CaseFunctionCall(List<Term> arguments)
    {
        super();
        
        assert (arguments != null);
        
        try
        {
            this.setFunction((CaseFunction) FunctionRegistry.getInstance().getFunction(CaseFunction.NAME));
        }
        catch (FunctionRegistryException e)
        {
            // This should never happen
            assert (false);
        }
        
        for (Term arg : arguments)
            this.addArgument(arg);
    }
    
    /**
     * Default constructor.
     */
    @SuppressWarnings("unused")
    private CaseFunctionCall()
    {
        super();
    }
    
    /**
     * Constructs the case function call for a variable number of arguments. The constructor copies the arguments into an internal
     * list. This constructor is mainly used in test cases.
     * 
     * @param arguments
     *            the function arguments.
     */
    public CaseFunctionCall(Term... arguments)
    {
        this(Arrays.asList(arguments));
    }
    
    @Override
    public Type inferType(List<Operator> operators) throws QueryCompilationException
    {
        Type inferred_type = null;
        
        // Make sure the number of arguments is even
        assert (this.getArguments().size() % 2 == 0);
        
        Iterator<Term> iter_args = this.getArguments().iterator();
        int position = 1;
        while (iter_args.hasNext())
        {
            Term when = iter_args.next();
            Type when_type = when.inferType(operators);
            
            // Invalid WHEN type
            if (!(when_type instanceof BooleanType) && !(when_type instanceof UnknownType))
            {
                throw new QueryCompilationException(QueryCompilation.INVALID_CASE_CONDITION_TYPE, this.getLocation(), position);
            }
            
            Term then = iter_args.next();
            Type then_type = then.inferType(operators);
            
            // Invalid THEN type
//            if (!(then_type instanceof ScalarType) && !(then_type instanceof NullType))
//            {
//                throw new QueryCompilationException(QueryCompilation.NON_SCALAR_CASE_BRANCH_TYPE, this.getLocation(), position);
//            }
            
            if (!(then_type instanceof NullType))
            {
                if (inferred_type == null)
                {
                    inferred_type = /*(ScalarType)*/ TypeUtil.cloneNoParent(then_type);
                }
                else
                {
                    // Branch does not have the same return type
                    if (inferred_type.getClass() != then_type.getClass())
                    {
                        throw new QueryCompilationException(QueryCompilation.INCONSISTENT_CASE_BRANCH_TYPE, this.getLocation(),
                                                            position);
                    }
                }
            }
            position++;
        }
        
        // FIXME Throw an exception that a type could not be inferred
        assert (inferred_type != null);
        
        this.setType(inferred_type);
        
        return this.getType();
    }
    
    @Override
    public String toExplainString()
    {
        String str = "CASE";
        Iterator<Term> iter_args = this.getArguments().iterator();
        while (iter_args.hasNext())
        {
            Term when = iter_args.next();
            str += " WHEN " + when.toExplainString();
            Term then = iter_args.next();
            str += " THEN " + then.toExplainString();
        }
        str += " END";
        
        return str;
    }
    
    @Override
    public Term copy()
    {
        List<Term> copies = new ArrayList<Term>();
        for (Term arg : this.getArguments())
        {
            copies.add(arg.copy());
        }
        CaseFunctionCall copy = new CaseFunctionCall(copies);
        super.copy(copy);
        
        return copy;
    }
    
}
