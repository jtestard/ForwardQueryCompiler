/**
 * 
 */
package edu.ucsd.forward.query.function.math;

import java.math.BigDecimal;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.FloatType;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.LongType;
import edu.ucsd.forward.data.type.TypeConverter;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.type.TypeException;
import edu.ucsd.forward.data.value.DecimalValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.PrimitiveValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * The MOD function divides two input arguments.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class ModFunction extends AbstractMathFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(ModFunction.class);
    
    public static final String  NAME = "MOD";
    
    /**
     * The signatures of the function.
     * 
     * @author Michalis Petropoulos
     */
    private enum FunctionSignatureName
    {
        MOD_INTEGER,

        MOD_LONG,

        MOD_DECIMAL;
    }
    
    /**
     * The default constructor.
     */
    public ModFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String left_name = "dividend";
        String right_name = "divisor";
        
        signature = new FunctionSignature(FunctionSignatureName.MOD_INTEGER.name(), TypeEnum.INTEGER.get());
        signature.addArgument(left_name, TypeEnum.INTEGER.get());
        signature.addArgument(right_name, TypeEnum.INTEGER.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MOD_LONG.name(), TypeEnum.LONG.get());
        signature.addArgument(left_name, TypeEnum.LONG.get());
        signature.addArgument(right_name, TypeEnum.LONG.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MOD_DECIMAL.name(), TypeEnum.DECIMAL.get());
        signature.addArgument(left_name, TypeEnum.DECIMAL.get());
        signature.addArgument(right_name, TypeEnum.DECIMAL.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        FunctionSignatureName sig = FunctionSignatureName.valueOf(call.getFunctionSignature().getName());
        
        List<Value> arg_values = evaluateArgumentsAndMatchFunctionSignature(call, input);
        Value left_value = arg_values.get(0);
        Value right_value = arg_values.get(1);
        
        // Handle NULL arguments
        if (left_value instanceof NullValue || right_value instanceof NullValue)
        {
            switch (sig)
            {
                case MOD_INTEGER:
                    return new BindingValue(new NullValue(IntegerType.class), true);
                case MOD_LONG:
                    return new BindingValue(new NullValue(LongType.class), true);
                case MOD_DECIMAL:
                    return new BindingValue(new NullValue(FloatType.class), true);
                default:
                    throw new AssertionError();
            }
        }
        
        Number left = (Number) ((PrimitiveValue<?>) left_value).getObject();
        Number right = (Number) ((PrimitiveValue<?>) right_value).getObject();
        
        try
        {
            BigDecimal left_decimal;
            BigDecimal right_decimal;
            Value result;
            switch (sig)
            {
                case MOD_INTEGER:
                    left_decimal = new BigDecimal((Integer) left);
                    right_decimal = new BigDecimal((Integer) right);
                    
                    result = new DecimalValue(left_decimal.remainder(right_decimal));
                    
                    return new BindingValue(TypeConverter.getInstance().convert(result, TypeEnum.INTEGER.get()), true);
                case MOD_LONG:
                    left_decimal = new BigDecimal((Long) left);
                    right_decimal = new BigDecimal((Long) right);
                    
                    result = new DecimalValue(left_decimal.remainder(right_decimal));
                    
                    return new BindingValue(TypeConverter.getInstance().convert(result, TypeEnum.LONG.get()), true);
                case MOD_DECIMAL:
                    left_decimal = (BigDecimal) left;
                    right_decimal = (BigDecimal) right;
                    
                    result = new DecimalValue(left_decimal.remainder(right_decimal));
                    
                    return new BindingValue(TypeConverter.getInstance().convert(result, TypeEnum.FLOAT.get()), true);
                default:
                    throw new AssertionError();
            }
        }
        catch (ArithmeticException e)
        {
            throw new QueryExecutionException(QueryExecution.ZERO_DIVISOR_DIV);
        }
        catch (TypeException e)
        {
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.FUNCTION_EVAL_ERROR, e, this.getName());
        }
    }
    
}
