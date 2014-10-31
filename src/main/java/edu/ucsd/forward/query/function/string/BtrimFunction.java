/**
 * 
 */
package edu.ucsd.forward.query.function.string;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * The btrim function.
 * 
 * @author Erick Zamora
 * 
 */
public class BtrimFunction extends AbstractStringFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(BtrimFunction.class);
    
    public static final String  NAME = "btrim";
    
    /**
     * The signatures of the function.
     * 
     * @author Erick Zamora
     */
    private enum FunctionSignatureName
    {
        BTRIM, BTRIM_CHARACTERS;
    }
    
    /**
     * The default constructor.
     */
    public BtrimFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String arg1_name = "string";
        String arg2_name = "string";
        
        signature = new FunctionSignature(FunctionSignatureName.BTRIM.name(), TypeEnum.STRING.get());
        signature.addArgument(arg1_name, TypeEnum.STRING.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.BTRIM_CHARACTERS.name(), TypeEnum.STRING.get());
        signature.addArgument(arg1_name, TypeEnum.STRING.get());
        signature.addArgument(arg2_name, TypeEnum.STRING.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        List<Value> arg_values = evaluateArgumentsAndMatchFunctionSignature(call, input);
        Value str_value_1 = arg_values.get(0);
        // Handle NULL argument
        if (str_value_1 instanceof NullValue) return new BindingValue(new NullValue(StringType.class), true);
        String str_1 = ((StringValue) str_value_1).getObject();
        
        String str_2 = null;
        if (call.getArguments().size() == 2)
        {
            Value str_value_2 = arg_values.get(1);
            // Handle NULL argument
            if (str_value_2 instanceof NullValue) return new BindingValue(new NullValue(StringType.class), true);
            str_2 = ((StringValue) str_value_2).getObject();
        }
        
        String result = null;
        if (str_2 != null)
        {
            // Replace all first and last occurrences of str_2
            result = (str_1.replaceAll("^(" + str_2 + ")*", "")).replaceAll("(" + str_2 + ")*$", "");
        }
        else
        {
            result = str_1.trim();
        }
        
        return new BindingValue(new StringValue(result), true);
    }
}
