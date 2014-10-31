/**
 * 
 */
package edu.ucsd.forward.query.function.string;

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
 * The upper string function.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class UpperFunction extends AbstractStringFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(UpperFunction.class);
    
    public static final String  NAME = "upper";
    
    /**
     * The signatures of the function.
     * 
     * @author Michalis Petropoulos
     */
    private enum FunctionSignatureName
    {
        UPPER_STRING;
    }
    
    /**
     * The default constructor.
     */
    public UpperFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String arg_name = "value";
        
        signature = new FunctionSignature(FunctionSignatureName.UPPER_STRING.name(), TypeEnum.STRING.get());
        signature.addArgument(arg_name, TypeEnum.STRING.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        Value value = evaluateArgumentsAndMatchFunctionSignature(call, input).get(0);
        
        // Handle NULL argument
        if (value instanceof NullValue) return new BindingValue(new NullValue(StringType.class), true);
        
        String result = ((StringValue) value).toString().toUpperCase();
        
        return new BindingValue(new StringValue(result), true);
    }
    
}
