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
 * The position function.
 * 
 * @author Erick Zamora
 * 
 */
public class PositionFunction extends AbstractStringFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(PositionFunction.class);
    
    public static final String  NAME = "position";
    
    /**
     * The signatures of the function.
     * 
     * @author Erick Zamora
     */
    private enum FunctionSignatureName
    {
        POSITION_STRING;
    }
    
    /**
     * The default constructor.
     */
    public PositionFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String arg1_name = "substring";
        String arg2_name = "in";
        
        signature = new FunctionSignature(FunctionSignatureName.POSITION_STRING.name(), TypeEnum.STRING.get());
        signature.addArgument(arg1_name, TypeEnum.STRING.get());
        signature.addArgument(arg2_name, TypeEnum.STRING.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        Value substring = evaluateArgumentsAndMatchFunctionSignature(call, input).get(0);
        Value string = evaluateArgumentsAndMatchFunctionSignature(call, input).get(1);
        
        // Handle NULL arguments
        if (substring instanceof NullValue || string instanceof NullValue)
        {
            return new BindingValue(new NullValue(IntegerType.class), false);
        }
        
        int result = ((StringValue) string).toString().indexOf(((StringValue) substring).toString()) + 1;
        
        return new BindingValue(new IntegerValue(result), false);
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        // FIXME : The position function can be pushed to the database, but because of the query processor pushes the position
        // function as position(var1, var2) instead of the necessary position(var1 in var2) it will fail. See Dx
        return false;
    }
}
