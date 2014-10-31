/**
 * 
 */
package edu.ucsd.forward.query.function.math;

import java.math.BigDecimal;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.DecimalType;
import edu.ucsd.forward.data.type.DoubleType;
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
import edu.ucsd.forward.query.function.Function;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * The MULT function multiplies two input arguments.
 * 
 * @author Yupeng
 * 
 */
public class MultFunction extends AbstractMathFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(MultFunction.class);
    
    public static final String  NAME = "*";
    
    /**
     * The signatures of the function.
     * 
     * @author Michalis Petropoulos
     */
    private enum FunctionSignatureName
    {
        MULT_INTEGER,

        MULT_LONG,

        MULT_FLOAT,

        MULT_DOUBLE,

        MULT_DECIMAL;
    }
    
    /**
     * The default constructor.
     */
    public MultFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String left_name = "left";
        String right_name = "right";
        
        signature = new FunctionSignature(FunctionSignatureName.MULT_INTEGER.name(), TypeEnum.INTEGER.get());
        signature.addArgument(left_name, TypeEnum.INTEGER.get());
        signature.addArgument(right_name, TypeEnum.INTEGER.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MULT_LONG.name(), TypeEnum.LONG.get());
        signature.addArgument(left_name, TypeEnum.LONG.get());
        signature.addArgument(right_name, TypeEnum.LONG.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MULT_FLOAT.name(), TypeEnum.FLOAT.get());
        signature.addArgument(left_name, TypeEnum.FLOAT.get());
        signature.addArgument(right_name, TypeEnum.FLOAT.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MULT_DOUBLE.name(), TypeEnum.DOUBLE.get());
        signature.addArgument(left_name, TypeEnum.DOUBLE.get());
        signature.addArgument(right_name, TypeEnum.DOUBLE.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MULT_DECIMAL.name(), TypeEnum.DECIMAL.get());
        signature.addArgument(left_name, TypeEnum.DECIMAL.get());
        signature.addArgument(right_name, TypeEnum.DECIMAL.get());
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
        FunctionSignatureName sig = FunctionSignatureName.valueOf(call.getFunctionSignature().getName());
        
        List<Value> arg_values = evaluateArgumentsAndMatchFunctionSignature(call, input);
        Value left_value = arg_values.get(0);
        Value right_value = arg_values.get(1);
        
        // Handle NULL arguments
        if (left_value instanceof NullValue || right_value instanceof NullValue)
        {
            switch (sig)
            {
                case MULT_INTEGER:
                    return new BindingValue(new NullValue(IntegerType.class), true);
                case MULT_LONG:
                    return new BindingValue(new NullValue(LongType.class), true);
                case MULT_FLOAT:
                    return new BindingValue(new NullValue(FloatType.class), true);
                case MULT_DOUBLE:
                    return new BindingValue(new NullValue(DoubleType.class), true);
                case MULT_DECIMAL:
                    return new BindingValue(new NullValue(DecimalType.class), true);
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
                case MULT_INTEGER:
                    left_decimal = new BigDecimal((Integer) left);
                    right_decimal = new BigDecimal((Integer) right);
                    
                    result = new DecimalValue(left_decimal.multiply(right_decimal));
                    
                    return new BindingValue(TypeConverter.getInstance().convert(result, TypeEnum.INTEGER.get()), true);
                case MULT_LONG:
                    left_decimal = new BigDecimal((Long) left);
                    right_decimal = new BigDecimal((Long) right);
                    
                    result = new DecimalValue(left_decimal.multiply(right_decimal));
                    
                    return new BindingValue(TypeConverter.getInstance().convert(result, TypeEnum.LONG.get()), true);
                case MULT_FLOAT:
                    left_decimal = new BigDecimal((Float) left);
                    right_decimal = new BigDecimal((Float) right);
                    
                    result = new DecimalValue(left_decimal.multiply(right_decimal));
                    
                    return new BindingValue(TypeConverter.getInstance().convert(result, TypeEnum.FLOAT.get()), true);
                case MULT_DOUBLE:
                    left_decimal = new BigDecimal((Double) left);
                    right_decimal = new BigDecimal((Double) right);
                    
                    result = new DecimalValue(left_decimal.multiply(right_decimal));
                    
                    return new BindingValue(TypeConverter.getInstance().convert(result, TypeEnum.DOUBLE.get()), true);
                case MULT_DECIMAL:
                    left_decimal = (BigDecimal) left;
                    right_decimal = (BigDecimal) right;
                    
                    result = new DecimalValue(left_decimal.multiply(right_decimal));
                    
                    return new BindingValue(result, true);
                default:
                    throw new AssertionError();
            }
        }
        catch (TypeException e)
        {
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.FUNCTION_EVAL_ERROR, e, this.getName());
        }
    }
    
}
