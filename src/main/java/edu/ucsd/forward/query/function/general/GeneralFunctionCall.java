package edu.ucsd.forward.query.function.general;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.*;
import edu.ucsd.forward.exception.ExceptionMessages.QueryCompilation;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.function.AbstractFunctionCall;
import edu.ucsd.forward.query.function.FunctionRegistry;
import edu.ucsd.forward.query.function.FunctionRegistryException;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.FunctionSignature.Argument;
import edu.ucsd.forward.query.function.FunctionSignature.Occurrence;
import edu.ucsd.forward.query.function.cast.CastFunctionCall;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.term.Term;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * A general function call, which has a function definition and a list of arguments. The arguments are terms, which can be other
 * function calls, query paths or constants.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class GeneralFunctionCall extends AbstractFunctionCall<GeneralFunction>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(GeneralFunctionCall.class);
    
    /**
     * Constructs the general function call for a given function definition and a list of arguments.
     * 
     * @param function_name
     *            a general function name.
     * @param arguments
     *            the function arguments.
     * @throws FunctionRegistryException
     *             if the function name cannot be resolved.
     */
    public GeneralFunctionCall(String function_name, List<Term> arguments) throws FunctionRegistryException
    {
        super();
        
        this.setFunction((GeneralFunction) FunctionRegistry.getInstance().getFunction(function_name));
        
        assert (arguments != null);
        for (Term arg : arguments)
            this.addArgument(arg);
    }
    
    /**
     * Constructs the general function call for a given function definition and a list of arguments.
     * 
     * @param function
     *            a general function definition.
     * @param arguments
     *            the function arguments.
     */
    public GeneralFunctionCall(GeneralFunction function, List<Term> arguments)
    {
        super();
        
        this.setFunction(function);
        
        assert (arguments != null);
        for (Term arg : arguments)
            this.addArgument(arg);
    }
    
    /**
     * Constructs the general function call for a given general function definition and a variable number of arguments. The
     * constructor copies the arguments into an internal list. This constructor is mainly used in test cases.
     * 
     * @param function_name
     *            a general function name.
     * @param arguments
     *            the function arguments.
     * @throws FunctionRegistryException
     *             if the function name cannot be resolved.
     */
    public GeneralFunctionCall(String function_name, Term... arguments) throws FunctionRegistryException
    {
        this(function_name, Arrays.asList(arguments));
    }
    
    /**
     * Constructs the general function call for a given general function definition and a variable number of arguments. The
     * constructor copies the arguments into an internal list. This constructor is mainly used in test cases.
     * 
     * @param function
     *            a general function definition.
     * @param arguments
     *            the function arguments.
     */
    public GeneralFunctionCall(GeneralFunction function, Term... arguments)
    {
        this(function, Arrays.asList(arguments));
    }
    
    /**
     * Default constructor.
     */
    @SuppressWarnings("unused")
    private GeneralFunctionCall()
    {
    }
    
    @Override
    public Type inferType(List<Operator> operators) throws QueryCompilationException
    {
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
            
            // Check if the provided type can be converted to the expected type
            Iterator<Argument> iter_expected = signature.getArguments().iterator();
            Argument arg = null;
            for (Type provided : arguments_types)
            {
                if (arg == null || arg.getOccurrence() == Occurrence.ONE)
                {
                    if (!iter_expected.hasNext())
                    {
                        found = false;
                        break;
                    }
                    else
                    {
                        arg = iter_expected.next();
                    }
                }
                
                Type expected = arg.getType();
                
                if (!TypeConverter.getInstance().canImplicitlyConvert(provided, expected))
                {
                    found = false;
                    break;
                }
            }
            
            // Set the function signature of the function call, if there are no other expected arguments
            if (found && !iter_expected.hasNext())
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
                if (argument_type instanceof JsonType || argument_type instanceof UnknownType)
                {
                    Type type = new UnknownType();
                    if (getFunction().getFunctionSignatures().size() == 1)
                    {
                        type = getFunction().getFunctionSignatures().get(0).getReturnType();
                    }
                    setType(type);
                    return type;
                }
            }
            throw new QueryCompilationException(QueryCompilation.NO_FUNCTION_SIGNATURE, this.getLocation(), this.toString());
        }
        
        // Insert cast function calls if necessary
        Iterator<Argument> iter_expected = this.getFunctionSignature().getArguments().iterator();
        Argument arg = null;
        for (int i = 0; i < arguments_types.size(); i++)
        {
            Type provided = arguments_types.get(i);
            if (arg == null || arg.getOccurrence() == Occurrence.ONE)
            {
                arg = iter_expected.next();
            }
            
            Type expected = arg.getType();
            
            if (provided.getClass() != expected.getClass())
            {
                Term provided_arg = this.getArguments().get(i);
                int index = this.removeArgument(provided_arg);
                
                CastFunctionCall cast_call = new CastFunctionCall(provided_arg, TypeEnum.getEntry(expected));
                cast_call.inferType(operators);
                this.addArgument(index, cast_call);
            }
        }
        
        this.setType(this.getFunctionSignature().getReturnType());
        
        return this.getType();
    }
    
    @Override
    public String toExplainString()
    {
        return super.toExplainString();
    }
    
    @Override
    public Term copy()
    {
        List<Term> copies = new ArrayList<Term>();
        for (Term arg : this.getArguments())
        {
            copies.add(arg.copy());
        }
        GeneralFunctionCall copy = new GeneralFunctionCall(this.getFunction(), copies);
        super.copy(copy);
        
        return copy;
    }
    
    @Override
    public GeneralFunctionCall copyWithoutType()
    {
        List<Term> copies = new ArrayList<Term>();
        for (Term arg : this.getArguments())
        {
            copies.add(arg.copyWithoutType());
        }
        GeneralFunctionCall copy = new GeneralFunctionCall(this.getFunction(), copies);
        return copy;
    }
}
