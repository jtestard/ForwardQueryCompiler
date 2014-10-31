/**
 * 
 */
package edu.ucsd.forward.query.function.string;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.IntegerType;
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
 * The string length function.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class LengthFunction extends AbstractStringFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(LengthFunction.class);
    
    public static final String  NAME = "character_length";
    
    /**
     * The signatures of the function.
     * 
     * @author Michalis Petropoulos
     */
    private enum FunctionSignatureName
    {
        LENGTH_STRING;
    }
    
    /**
     * The default constructor.
     */
    public LengthFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String arg_name = "value";
        
        signature = new FunctionSignature(FunctionSignatureName.LENGTH_STRING.name(), TypeEnum.INTEGER.get());
        signature.addArgument(arg_name, TypeEnum.STRING.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        Value value = evaluateArgumentsAndMatchFunctionSignature(call, input).get(0);
        
        // Handle NULL argument
        if (value instanceof NullValue) return new BindingValue(new NullValue(IntegerType.class), true);
        
        int result = ((StringValue) value).toString().length();
        
        return new BindingValue(new IntegerValue(result), true);
    }
    
}
