/**
 * 
 */
package edu.ucsd.forward.query.function;

import java.util.HashMap;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.NullType;
import edu.ucsd.forward.exception.ExceptionMessages;
import edu.ucsd.forward.query.function.aggregate.AverageFunction;
import edu.ucsd.forward.query.function.aggregate.CountFunction;
import edu.ucsd.forward.query.function.aggregate.MaxFunction;
import edu.ucsd.forward.query.function.aggregate.MinFunction;
import edu.ucsd.forward.query.function.aggregate.NestFunction;
import edu.ucsd.forward.query.function.aggregate.StringAggFunction;
import edu.ucsd.forward.query.function.aggregate.SumFunction;
import edu.ucsd.forward.query.function.cast.CastFunction;
import edu.ucsd.forward.query.function.collection.CollectionFunction;
import edu.ucsd.forward.query.function.collection.CollectionInitFunction;
import edu.ucsd.forward.query.function.comparison.EqualFunction;
import edu.ucsd.forward.query.function.comparison.GreaterEqualFunction;
import edu.ucsd.forward.query.function.comparison.GreaterThanFunction;
import edu.ucsd.forward.query.function.comparison.IsNullFunction;
import edu.ucsd.forward.query.function.comparison.LessEqualFunction;
import edu.ucsd.forward.query.function.comparison.LessThanFunction;
import edu.ucsd.forward.query.function.comparison.NotEqualFunction;
import edu.ucsd.forward.query.function.conditional.CaseFunction;
import edu.ucsd.forward.query.function.custom.CustomJavaFunction;
import edu.ucsd.forward.query.function.custom.GrouponDealsFunction;
import edu.ucsd.forward.query.function.date.CurrentTimestampFunction;
import edu.ucsd.forward.query.function.date.TimestampToMsFunction;
import edu.ucsd.forward.query.function.email.EmailFunction;
import edu.ucsd.forward.query.function.json.JsonArrayGetByIndex;
import edu.ucsd.forward.query.function.json.JsonGetFunction;
import edu.ucsd.forward.query.function.logical.AndFunction;
import edu.ucsd.forward.query.function.logical.NotFunction;
import edu.ucsd.forward.query.function.logical.OrFunction;
import edu.ucsd.forward.query.function.math.AddFunction;
import edu.ucsd.forward.query.function.math.DivFunction;
import edu.ucsd.forward.query.function.math.HypotFunction;
import edu.ucsd.forward.query.function.math.ModFunction;
import edu.ucsd.forward.query.function.math.MultFunction;
import edu.ucsd.forward.query.function.math.NegFunction;
import edu.ucsd.forward.query.function.math.SubFunction;
import edu.ucsd.forward.query.function.metadata.SchemaObjectFunction;
import edu.ucsd.forward.query.function.pattern_matching.LikeFunction;
import edu.ucsd.forward.query.function.rest.RestFunction;
import edu.ucsd.forward.query.function.string.BtrimFunction;
import edu.ucsd.forward.query.function.string.ConcatFunction;
import edu.ucsd.forward.query.function.string.EscapeJavaScriptFunction;
import edu.ucsd.forward.query.function.string.EscapeXhtmlFunction;
import edu.ucsd.forward.query.function.string.EscapeXmlFunction;
import edu.ucsd.forward.query.function.string.FormatFunction;
import edu.ucsd.forward.query.function.string.LengthFunction;
import edu.ucsd.forward.query.function.string.LowerFunction;
import edu.ucsd.forward.query.function.string.PositionFunction;
import edu.ucsd.forward.query.function.string.SplitPartFunction;
import edu.ucsd.forward.query.function.string.SubstringFunction;
import edu.ucsd.forward.query.function.string.UpperFunction;
import edu.ucsd.forward.query.function.tuple.AssignIDToTupleFunction;
import edu.ucsd.forward.query.function.tuple.TupleFunction;

/**
 * Function library contains a set of <code>Function</code>s.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public final class FunctionRegistry
{
    @SuppressWarnings("unused")
    private static final Logger                        log                     = Logger.getLogger(FunctionRegistry.class);
    
    /**
     * The function registry per thread.
     */
    private static final ThreadLocal<FunctionRegistry> INSTANCE                = new ThreadLocal<FunctionRegistry>();
    
    /**
     * The common functions.
     */
    private static Map<String, Function>               s_common_functions      = new HashMap<String, Function>();
    
    /**
     * The application functions.
     */
    private Map<String, Function>                      m_application_functions = new HashMap<String, Function>();
    
    static
    {
        // String functions
        addCommonFunction(ConcatFunction.NAME, new ConcatFunction());
        addCommonFunction(LowerFunction.NAME, new LowerFunction());
        addCommonFunction(UpperFunction.NAME, new UpperFunction());
        addCommonFunction(LengthFunction.NAME, new LengthFunction());
        addCommonFunction(SubstringFunction.NAME, new SubstringFunction());
        addCommonFunction(BtrimFunction.NAME, new BtrimFunction());
        addCommonFunction(EscapeXmlFunction.NAME, new EscapeXmlFunction());
        addCommonFunction(EscapeXhtmlFunction.NAME, new EscapeXhtmlFunction());
        addCommonFunction(EscapeJavaScriptFunction.NAME, new EscapeJavaScriptFunction());
        addCommonFunction(FormatFunction.NAME, new FormatFunction());
        addCommonFunction(PositionFunction.NAME, new PositionFunction());
        addCommonFunction(SplitPartFunction.NAME, new SplitPartFunction());
        
        // Date/Time functions
        addCommonFunction(CurrentTimestampFunction.NAME, new CurrentTimestampFunction());
        addCommonFunction(TimestampToMsFunction.NAME, new TimestampToMsFunction());
        
        // Comparison functions
        addCommonFunction(EqualFunction.NAME, new EqualFunction());
        addCommonFunction(NotEqualFunction.NAME, new NotEqualFunction());
        addCommonFunction(IsNullFunction.NAME, new IsNullFunction());
        addCommonFunction(GreaterThanFunction.NAME, new GreaterThanFunction());
        addCommonFunction(LessEqualFunction.NAME, new LessEqualFunction());
        addCommonFunction(GreaterEqualFunction.NAME, new GreaterEqualFunction());
        addCommonFunction(LessThanFunction.NAME, new LessThanFunction());
        
        // Logical functions
        addCommonFunction(AndFunction.NAME, new AndFunction());
        addCommonFunction(OrFunction.NAME, new OrFunction());
        addCommonFunction(NotFunction.NAME, new NotFunction());
        
        // Math functions
        addCommonFunction(AddFunction.NAME, new AddFunction());
        addCommonFunction(DivFunction.NAME, new DivFunction());
        addCommonFunction(MultFunction.NAME, new MultFunction());
        addCommonFunction(SubFunction.NAME, new SubFunction());
        addCommonFunction(NegFunction.NAME, new NegFunction());
        addCommonFunction(ModFunction.NAME, new ModFunction());
        addCommonFunction(HypotFunction.NAME, new HypotFunction());
        
        // Pattern matching function
        addCommonFunction(LikeFunction.NAME, new LikeFunction());
        
        // Aggregate function
        addCommonFunction(MaxFunction.NAME, new MaxFunction());
        addCommonFunction(MinFunction.NAME, new MinFunction());
        addCommonFunction(CountFunction.NAME, new CountFunction());
        addCommonFunction(SumFunction.NAME, new SumFunction());
        addCommonFunction(AverageFunction.NAME, new AverageFunction());
        addCommonFunction(NestFunction.NAME, new NestFunction());
        addCommonFunction(StringAggFunction.NAME, new StringAggFunction());
        
        // Case functions
        addCommonFunction(CaseFunction.NAME, new CaseFunction());
        
        // Cast function
        addCommonFunction(CastFunction.NAME, new CastFunction());
        
        // Tuple function
        addCommonFunction(TupleFunction.NAME, new TupleFunction());
        addCommonFunction(AssignIDToTupleFunction.NAME, new AssignIDToTupleFunction());
        
        // Collection function
        addCommonFunction(CollectionFunction.NAME, new CollectionFunction());
        addCommonFunction(CollectionInitFunction.NAME, new CollectionInitFunction());
        
        // Metadata functions
        addCommonFunction(SchemaObjectFunction.NAME, new SchemaObjectFunction());
        

        // Email function
        addCommonFunction(EmailFunction.NAME, new EmailFunction());
        
        // Groupon functions
        addCommonFunction(GrouponDealsFunction.NAME, new GrouponDealsFunction());
        
        // Dummy custom function
        addCommonFunction(CustomJavaFunction.NAME, new CustomJavaFunction());
        
        // Testing function
        addCommonFunction(OutOfMemoryFunction.NAME, new OutOfMemoryFunction());
        
        // REST function
        addCommonFunction(RestFunction.NAME, new RestFunction());
        
        // JSON fucntions
        addCommonFunction(JsonGetFunction.NAME, new JsonGetFunction());
        addCommonFunction(JsonArrayGetByIndex.NAME, new JsonArrayGetByIndex());
    }
    
    /**
     * Private constructor.
     */
    protected FunctionRegistry()
    {
    }
    
    /**
     * Adds one function to the library.
     * 
     * @param function_name
     *            the function name to add to the library.
     * @param function
     *            the function to add to the library.
     */
    public static void addCommonFunction(String function_name, Function function)
    {
        assert (function_name != null);
        assert (function != null);
        
        String uname = function_name.toUpperCase();
        
        assert !(s_common_functions.containsKey(uname));
        s_common_functions.put(uname, function);
    }
    
    /**
     * Adds one application function to the library.
     * 
     * @param function
     *            the application function to add.
     * @throws FunctionRegistryException
     *             if function to add already exists.
     */
    public void addApplicationFunction(Function function) throws FunctionRegistryException
    {
        assert function != null;
        String uname = function.getName().toUpperCase();
        if (s_common_functions.containsKey(uname) || m_application_functions.containsKey(uname))
        {
            throw new FunctionRegistryException(ExceptionMessages.Function.DUPLICATE_FUNCTION, function.getName());
        }
        m_application_functions.put(uname, function);
    }
    
    /**
     * Removes one application function from the library.
     * 
     * @param name
     *            the name of the application function to remove.
     * @throws FunctionRegistryException
     *             if function to remove does not exist.
     */
    public void removeApplicationFunction(String name) throws FunctionRegistryException
    {
        assert name != null;
        String uname = name.toUpperCase();
        if (!m_application_functions.containsKey(uname))
        {
            throw new FunctionRegistryException(ExceptionMessages.Function.NON_EXISTING_FUNCTION, name);
        }
        m_application_functions.remove(uname);
    }
    
    /**
     * Gets all the common functions.
     * 
     * @return all the common functions.
     */
    public Map<String, Function> getCommonFunctions()
    {
        return s_common_functions;
    }
    
    /**
     * Gets all the application functions.
     * 
     * @return all the application functions.
     */
    public Map<String, Function> getApplicationFunctions()
    {
        return m_application_functions;
    }
    
    /**
     * Sets all the application functions. Used only during runtime and assumes that there is no duplicate function name.
     * 
     * @param functions
     *            the application functions to set.
     */
    public void setApplicationFunctions(Map<String, Function> functions)
    {
        assert (functions != null);
        m_application_functions = functions;
    }
    
    /**
     * Resets the application functions.
     */
    public void resetApplicationFunctions()
    {
        m_application_functions = new HashMap<String, Function>();
    }
    
    /**
     * Checks if the function with the specific name is an FPL function. Checks if the function with the specific name returns void.
     * 
     * @param name
     *            the function name
     * @return <code>true</code> if the function returns void, <code>false</code> otherwise.
     * @throws FunctionRegistryException
     *             if the function name cannot be resolved.
     */
    public boolean isVoidFunction(String name) throws FunctionRegistryException
    {
        FunctionSignature signature = FunctionRegistry.getInstance().getFunction(name).getFunctionSignatures().get(0);
        if (signature.getReturnType() instanceof NullType) return true;
        
        return false;
    }
    
    /**
     * Gets the static instance.
     * 
     * @return the static instance.
     */
    public static FunctionRegistry getInstance()
    {
        if (INSTANCE.get() == null)
        {
            INSTANCE.set(new FunctionRegistry());
        }
        
        return INSTANCE.get();
    }
    
    /**
     * Determines whether there is a function definition with the specific name.
     * 
     * @param name
     *            the function name.
     * @return true, if there is a function definition with the specific name; otherwise, false.
     */
    public boolean hasFunction(String name)
    {
        String uname = name.toUpperCase();
        if (s_common_functions.containsKey(uname) || m_application_functions.containsKey(uname))
        {
            return true;
        }
        
        return false;
    }
    
    /**
     * Gets the function definition with the specific name.
     * 
     * @param name
     *            the function name.
     * @return the specified function definition.
     * @throws FunctionRegistryException
     *             if the function name cannot be resolved.
     */
    public Function getFunction(String name) throws FunctionRegistryException
    {
        String uname = name.toUpperCase();
        
        if (m_application_functions.containsKey(uname)) return m_application_functions.get(uname);
        
        Function function = s_common_functions.get(uname);
        if (function == null)
        {
            throw new FunctionRegistryException(ExceptionMessages.Function.UNKNOWN_FUNCTION_NAME, name);
        }
        
        return function;
    }
    
}
