/**
 * 
 */
package edu.ucsd.forward.query.function;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeConverter;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.fpl.FplInterpreterFactory;
import edu.ucsd.forward.fpl.ast.Definition;
import edu.ucsd.forward.fpl.ast.FunctionDefinition;
import edu.ucsd.forward.fpl.ast.ParameterDeclaration;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.FunctionSignature.Argument;
import edu.ucsd.forward.query.function.FunctionSignature.Occurrence;
import edu.ucsd.forward.query.function.logical.AndFunction;
import edu.ucsd.forward.query.function.logical.OrFunction;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.TermEvaluator;

/**
 * An abstract implementation of the function definition interface.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractFunction implements Function
{
    private String                  m_name;
    
    private List<FunctionSignature> m_signatures;
    
    /**
     * The data source that can evaluate this function. Can either be both mediator and database or only one of the two
     */
    protected String                m_data_source;
    
    /**
     * Constructor.
     * 
     * @param name
     *            the name of the function.
     */
    protected AbstractFunction(String name)
    {
        m_name = name;
        m_signatures = new ArrayList<FunctionSignature>();
    }
    
    /**
     * Constructs the function with the name and definition for the signature.
     * 
     * @param name
     *            the name of the function.
     * @param definition
     *            the definition of the signature
     */
    protected AbstractFunction(String name, String definition)
    {
        this(name);
        addFunctionSignature(parseSignature(definition));
    }
    
    /**
     * Parses the function definition to get the function signature.
     * 
     * @param definition_str
     *            the definition of the function in string representation.
     * @return the parsed signature
     */
    private FunctionSignature parseSignature(String definition_str)
    {
        try
        {
            List<Definition> definitions = FplInterpreterFactory.getInstance().parseFplCode(definition_str, new LocationImpl());
            assert definitions.size() == 1;
            FunctionDefinition definition = (FunctionDefinition) definitions.get(0);
            Type return_type = definition.getReturnType();
            FunctionSignature signature = new FunctionSignature(m_name, return_type);
            for (ParameterDeclaration declaration : definition.getParameterDeclarations())
            {
                signature.addArgument(declaration.getName(), declaration.getType());
            }
            return signature;
        }
        catch (Exception e)
        {
            // Should not happen
            throw new AssertionError(e);
        }
    }
    
    @Override
    public String getName()
    {
        return m_name;
    }
    
    @Override
    public Notation getNotation()
    {
        return Function.Notation.PREFIX;
    }
    
    /**
     * @return the data_source
     */
    @Override
    public String getDataSourceName()
    {
        if (!isSqlCompliant()) return DataSource.MEDIATOR;
        return m_data_source;
    }
    
    /**
     * @param data_source
     *            the data_source to set
     */
    @Override
    public void setDataSource(String data_source)
    {
        m_data_source = data_source;
    }
    
    @Override
    public List<FunctionSignature> getFunctionSignatures()
    {
        return Collections.unmodifiableList(m_signatures);
    }
    
    /**
     * Adds a function signature.
     * 
     * @param signature
     *            the function signature to add.
     */
    public void addFunctionSignature(FunctionSignature signature)
    {
        assert (signature != null);
        
        m_signatures.add(signature);
    }
    
    /**
     * Evaluates the function arguments. Also matches the function signature at runtime if it does not exist,given the argument
     * values.
     * 
     * @param call
     *            the function call.
     * @param binding
     *            the binding holding the arguments to the function.
     * @return the argument values.
     * @throws QueryExecutionException
     *             if the function signature cannot be matched.
     */
    protected List<Value> evaluateArgumentsAndMatchFunctionSignature(FunctionCall<?> call, Binding binding)
            throws QueryExecutionException
    {
        // Special handling for AND and OR functions, because they support short-circuiting, in which case not all arguments should
        // be evaluated.
        if (this instanceof AndFunction || this instanceof OrFunction) return Collections.<Value> emptyList();
        
        List<Class<? extends Type>> arguments_types = new ArrayList<Class<? extends Type>>();
        List<BindingValue> binding_values = new ArrayList<BindingValue>();
        for (Term arg : call.getArguments())
        {
            BindingValue value = TermEvaluator.evaluate(arg, binding);
            binding_values.add(value);
            Value sql_value;
            sql_value = value.getSqlValue();
            arguments_types.add(sql_value.getTypeClass());
        }
        
        if (call.getFunctionSignature() == null)
        {
            // Find the first matching signature
            for (FunctionSignature signature : this.getFunctionSignatures())
            {
                boolean found = true;
                
                // Check if the provided type can be converted to the expected type
                Iterator<Argument> iter_expected = signature.getArguments().iterator();
                Argument arg = null;
                for (Class<? extends Type> provided : arguments_types)
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
                    
                    Class<? extends Type> expected = arg.getType().getClass();
                    
                    if (!TypeConverter.getInstance().canImplicitlyConvert(provided, expected))
                    {
                        found = false;
                        break;
                    }
                }
                
                // Set the function signature of the function call, if there are no other expected arguments
                if (found && !iter_expected.hasNext())
                {
                    ((AbstractFunctionCall<?>) call).setFunctionSignature(signature);
                    break;
                }
            }
            
            // No function signature found
            if (call.getFunctionSignature() == null)
            {
                throw new QueryExecutionException(QueryExecution.NO_FUNCTION_SIGNATURE, call.getLocation(), this.toString());
            }
        }
        
        // Set the evaluated arguments
        List<Value> argument_values = new ArrayList<Value>();
        FunctionSignature sig = call.getFunctionSignature();
        for (int i = 0; i < binding_values.size(); i++)
        {
            Type argument_type = null;
            if (sig.getArguments().size() > i) argument_type = sig.getArguments().get(i).getType();
            else
            {
                Argument last_argument = sig.getArguments().get(sig.getArguments().size() - 1);
                if (last_argument.getOccurrence() != Occurrence.MULTIPLE)
                {
                    throw new QueryExecutionException(QueryExecution.NO_FUNCTION_SIGNATURE, call.getLocation(), this.toString());
                }
                argument_type = last_argument.getType();
            }
            Value sql_value = binding_values.get(i).getSqlValue(argument_type);
            argument_values.add(sql_value);
        }
        
        return argument_values;
    }
    
}
