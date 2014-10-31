package edu.ucsd.forward.query.function.external;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.function.AbstractFunctionCall;
import edu.ucsd.forward.query.function.FunctionSignature.Argument;
import edu.ucsd.forward.query.function.FunctionSignature.Occurrence;
import edu.ucsd.forward.query.function.cast.CastFunctionCall;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.term.Term;

/**
 * A call to an external function.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class ExternalFunctionCall extends AbstractFunctionCall<ExternalFunction>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ExternalFunctionCall.class);
    
    private ExternalFunction    m_function;
    
    /**
     * Constructs the external function call for a given function definition and a list of arguments.
     * 
     * @param function
     *            an external function definition.
     * @param arguments
     *            the function arguments.
     */
    public ExternalFunctionCall(ExternalFunction function, List<Term> arguments)
    {
        super();
        
        this.setFunction(function);
        m_function = function;
        
        assert (arguments != null);
        for (Term arg : arguments)
            this.addArgument(arg);
    }
    
    /**
     * Constructs the external function call for a given general function definition and a variable number of arguments. The
     * constructor copies the arguments into an internal list. This constructor is mainly used in test cases.
     * 
     * @param function
     *            an external function definition.
     * @param arguments
     *            the function arguments.
     */
    public ExternalFunctionCall(ExternalFunction function, Term... arguments)
    {
        this(function, Arrays.asList(arguments));
    }
    
    @Override
    public ExternalFunction getFunction()
    {
        return m_function;
    }
    
    @Override
    public Type inferType(List<Operator> operators) throws QueryCompilationException
    {
        assert (this.getFunction().getFunctionSignatures().size() == 1);
        this.setFunctionSignature(this.getFunction().getFunctionSignatures().get(0));
        
        // Get the output types of the arguments
        List<Type> arguments_types = new ArrayList<Type>();
        for (Term arg : this.getArguments())
        {
            arguments_types.add(arg.inferType(operators));
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
                boolean compliant = this.getFunction().getTargetDataSource().equals(metadata.getName());
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
    public String toExplainString()
    {
        String str = this.getFunction().getName() + "(";
        
        for (Term argument : this.getArguments())
            str += argument.toExplainString() + ", ";
        if (!this.getArguments().isEmpty()) str = str.substring(0, str.length() - 2);
        str += ")";
        
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
        ExternalFunctionCall copy = new ExternalFunctionCall(this.getFunction(), copies);
        super.copy(copy);
        
        return copy;
    }
    
}
