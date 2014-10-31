/**
 * 
 */
package edu.ucsd.forward.query.function.logical;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.Function;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * The NOT function.
 * 
 * @author Yupeng
 * 
 */
public class NotFunction extends AbstractLogicalFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(NotFunction.class);
    
    public static final String  NAME = "NOT";
    
    /**
     * The signatures of the function.
     * 
     * @author Michalis Petropoulos
     */
    private enum FunctionSignatureName
    {
        NOT_BOOLEAN;
    }
    
    /**
     * The default constructor.
     */
    public NotFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String arg_name = "value";
        
        signature = new FunctionSignature(FunctionSignatureName.NOT_BOOLEAN.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(arg_name, TypeEnum.BOOLEAN.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public Notation getNotation()
    {
        return Function.Notation.PREFIX;
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        Value value = evaluateArgumentsAndMatchFunctionSignature(call, input).get(0);
        
        // Handle NULL argument
        if (value instanceof NullValue)
        {
            return new BindingValue(new NullValue(BooleanType.class), true);
        }
        BooleanValue bvalue = (BooleanValue) value;
        
        boolean result = !bvalue.getObject();
        return new BindingValue(new BooleanValue(result), true);
    }
    
}
