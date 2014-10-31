package edu.ucsd.forward.query.function.aggregate;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.JsonType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.UnknownType;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.ExceptionMessages.QueryCompilation;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.ast.SetQuantifier;
import edu.ucsd.forward.query.function.AbstractFunctionCall;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.TermEvaluator;

/**
 * An aggregate function call, which has a aggregate function definition and a list of arguments. The arguments are terms, which can
 * be other function calls, query paths or constants.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class AggregateFunctionCall extends AbstractFunctionCall<AggregateFunction>
{
    @SuppressWarnings("unused")
    private static final Logger log      = Logger.getLogger(AggregateFunctionCall.class);
    
    private static final String ASTERISK = "*";
    
    private SetQuantifier       m_set_quantifier;
    
    /**
     * Constructs the aggregate function call for a given aggregate function definition, a set quantifier and an argument.
     * 
     * @param function
     *            the aggregate function definition.
     * @param set_quantifier
     *            a set quantifier.
     * @param arguments
     *            the function arguments.
     */
    public AggregateFunctionCall(AggregateFunction function, SetQuantifier set_quantifier, List<Term> arguments)
    {
        super();
        
        assert (function != null);
        this.setFunction(function);
        
        assert (set_quantifier != null);
        m_set_quantifier = set_quantifier;
        
        // Accommodate COUNT(*)
        if (arguments.isEmpty())
        {
            assert (function instanceof CountFunction) || (function instanceof NestFunction);
        }
        else
        {
            if (arguments.size() > 1)
            {
                assert (function instanceof NestFunction || function instanceof StringAggFunction);
            }
            for (Term argument : arguments)
            {
                assert argument != null;
                this.addArgument(argument);
            }
        }
    }
    
    /**
     * Default constructor.
     */
    @SuppressWarnings("unused")
    private AggregateFunctionCall()
    {
        super();
    }
    
    @Override
    public Type inferType(List<Operator> operators) throws QueryCompilationException
    {
        if (getFunction() instanceof NestFunction)
        {
            // Construct the output collection type
            CollectionType collection_type = new CollectionType();
            assert (operators.size() == 1);
            
            for (Term arg : this.getArguments())
            {
                arg.inferType(operators);
                collection_type.getTupleType().setAttributeNoCheck(arg.getDefaultProjectAlias(),
                                                                   TypeUtil.cloneNoParent(arg.getType()));
            }
            
            this.setType(collection_type);
            
            return this.getType();
        }
        
        // Get the output types of the arguments
        List<Type> arguments_types = new ArrayList<Type>();
        for (Term arg : this.getArguments())
        {
            arguments_types.add(arg.inferType(operators));
        }
        
        // Find the first matching signature
        for (FunctionSignature signature : this.getFunction().getFunctionSignatures())
        {
            boolean found = true;
            
            // Make sure the signatures have the same arity
            if (signature.getArguments().size() != arguments_types.size()) continue;
            
            // Check if the provided type can be converted to the expected type
            for (int i = 0; i < arguments_types.size(); i++)
            {
                Type provided = arguments_types.get(i);
                Type expected = signature.getArguments().get(i).getType();
                
                if (provided.getClass() != expected.getClass())
                {
                    found = false;
                    break;
                }
            }
            
            // Set the function signature of the function call
            if (found)
            {
                this.setFunctionSignature(signature);
                break;
            }
        }
        
        // No function signature found
        if (this.getFunctionSignature() == null)
        {
            // Check if any argument type is Java type or unknown type
            for (Type argument_type : arguments_types)
            {
                if (argument_type instanceof UnknownType || argument_type instanceof JsonType)
                {
                    UnknownType type = new UnknownType();
                    setType(type);
                    return type;
                }
            }
            throw new QueryCompilationException(QueryCompilation.NO_FUNCTION_SIGNATURE, this.getLocation(),
                                                this.getFunction().getName());
        }
        
        this.setType(this.getFunctionSignature().getReturnType());
        
        return this.getType();
    }
    
    /**
     * Matches the function signature at runtime,given the argument values.
     * 
     * @param bindings
     *            the bindings holding the arguments to the function.
     * @throws QueryExecutionException
     *             if the function signature cannot be matched.
     */
    public void matchFunctionSignature(List<Binding> bindings) throws QueryExecutionException
    {
        List<Class<? extends Type>> arguments_types = new ArrayList<Class<? extends Type>>();
        for (Term arg : this.getArguments())
        {
            BindingValue value = TermEvaluator.evaluate(arg, bindings.get(0));
            Value sql_value;
            sql_value = value.getSqlValue();
            arguments_types.add(sql_value.getTypeClass());
        }
        
        // Find the first matching signature
        for (FunctionSignature signature : this.getFunction().getFunctionSignatures())
        {
            boolean found = true;
            
            // Make sure the signatures have the same arity
            if (signature.getArguments().size() != arguments_types.size()) continue;
            
            // Check if the provided type can be converted to the expected type
            for (int i = 0; i < arguments_types.size(); i++)
            {
                Class<? extends Type> provided = arguments_types.get(i);
                Type expected = signature.getArguments().get(i).getType();
                
                if (provided != expected.getClass())
                {
                    found = false;
                    break;
                }
            }
            
            // Set the function signature of the function call
            if (found)
            {
                this.setFunctionSignature(signature);
                break;
            }
        }
        
        // No function signature found
        if (this.getFunctionSignature() == null)
        {
            throw new QueryExecutionException(QueryExecution.NO_FUNCTION_SIGNATURE, this.getLocation(), this.toString());
        }
    }
    
    /**
     * Gets the set quantifier.
     * 
     * @return the set quantifier.
     */
    public SetQuantifier getSetQuantifier()
    {
        return m_set_quantifier;
    }
    
    @Override
    public String toExplainString()
    {
        String str = this.getFunction().getName() + "(";
        
        // Accommodate COUNT(*)
        if (!this.getArguments().isEmpty())
        {
            str += m_set_quantifier.name() + " ";
            for (Term term : this.getArguments())
            {
                str += term.toExplainString() + ", ";
            }
        }
        else
        {
            str += ASTERISK;
        }
        
        return str + ")";
    }
    
    @Override
    public Term copy()
    {
        List<Term> copies = new ArrayList<Term>();
        for (Term arg : this.getArguments())
        {
            copies.add(arg.copy());
        }
        AggregateFunctionCall copy = new AggregateFunctionCall(this.getFunction(), m_set_quantifier, copies);
        super.copy(copy);
        
        return copy;
    }
    
    @Override
    public AggregateFunctionCall copyWithoutType()
    {
        List<Term> copies = new ArrayList<Term>();
        for (Term arg : this.getArguments())
        {
            copies.add(arg.copyWithoutType());
        }
        AggregateFunctionCall copy = new AggregateFunctionCall(this.getFunction(), m_set_quantifier, copies);
        
        return copy;
    }
    
}
