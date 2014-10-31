/**
 * 
 */
package edu.ucsd.forward.query.function.string;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * The substring function.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class SubstringFunction extends AbstractStringFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(SubstringFunction.class);
    
    public static final String  NAME = "substring";
    
    /**
     * The signatures of the function.
     * 
     * @author Michalis Petropoulos
     */
    private enum FunctionSignatureName
    {
        SUBSTRING_FROM, SUBSTRING_FROM_FOR;
    }
    
    /**
     * The default constructor.
     */
    public SubstringFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String arg1_name = "string";
        String arg2_name = "from_position";
        String arg3_name = "for_length";
        
        signature = new FunctionSignature(FunctionSignatureName.SUBSTRING_FROM.name(), TypeEnum.STRING.get());
        signature.addArgument(arg1_name, TypeEnum.STRING.get());
        signature.addArgument(arg2_name, TypeEnum.INTEGER.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.SUBSTRING_FROM_FOR.name(), TypeEnum.STRING.get());
        signature.addArgument(arg1_name, TypeEnum.STRING.get());
        signature.addArgument(arg2_name, TypeEnum.INTEGER.get());
        signature.addArgument(arg3_name, TypeEnum.INTEGER.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        List<Value> arg_values = evaluateArgumentsAndMatchFunctionSignature(call, input);
        Value str_value = arg_values.get(0);
        // Handle NULL argument
        if (str_value instanceof NullValue) return new BindingValue(new NullValue(StringType.class), true);
        String str = ((StringValue) str_value).getObject();
        
        Value from_value = arg_values.get(1);
        // Handle NULL argument
        if (from_value instanceof NullValue) return new BindingValue(new NullValue(StringType.class), true);
        int from_position = ((IntegerValue) from_value).getObject();
        
        int for_length;
        if (call.getArguments().size() == 3)
        {
            Value for_value = arg_values.get(2);
            // Handle NULL argument
            if (for_value instanceof NullValue) return new BindingValue(new NullValue(StringType.class), true);
            for_length = ((IntegerValue) for_value).getObject();
        }
        else
        {
            for_length = (str.length() + 1 > from_position) ? str.length() + 1 : from_position;
        }
        
        int to_position = from_position + for_length;
        
        if (to_position < from_position)
        {
            // FIXME Throw a negative substring exception.
            assert (false);
        }
        
        // Return the empty string
        if (from_position > str.length() || to_position < 1)
        {
            return new BindingValue(new StringValue(""), true);
        }
        
        if (from_position < 1) from_position = 1;
        if (to_position > str.length() + 1) to_position = str.length() + 1;
        
        String result = str.substring(from_position - 1, to_position - 1);
        
        return new BindingValue(new StringValue(result), true);
    }
}
