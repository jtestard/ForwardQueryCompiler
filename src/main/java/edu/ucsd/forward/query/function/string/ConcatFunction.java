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
import edu.ucsd.forward.query.function.Function;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.FunctionSignature.Occurrence;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * The string concatenation function.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class ConcatFunction extends AbstractStringFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(ConcatFunction.class);
    
    public static final String  NAME = "||";
    
    /**
     * The signatures of the function.
     * 
     * @author Michalis Petropoulos
     */
    private enum FunctionSignatureName
    {
        CONCAT_STRING;
    }
    
    /**
     * The default constructor.
     */
    public ConcatFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String arg_name = "arg";
        
        signature = new FunctionSignature(FunctionSignatureName.CONCAT_STRING.name(), TypeEnum.STRING.get());
        signature.addArgument(arg_name, TypeEnum.STRING.get(), Occurrence.MULTIPLE);
        this.addFunctionSignature(signature);
    }
    
    @Override
    public Notation getNotation()
    {
        return Function.Notation.INFIX;
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        String result = "";
        for (Value value : evaluateArgumentsAndMatchFunctionSignature(call, input))
        {
            // Handle NULL argument
            if (value instanceof NullValue) return new BindingValue(new NullValue(StringType.class), true);
            
            StringValue string_value = (StringValue) value;
            result += string_value.toString();
        }
        return new BindingValue(new StringValue(result), true);
    }
    
}
