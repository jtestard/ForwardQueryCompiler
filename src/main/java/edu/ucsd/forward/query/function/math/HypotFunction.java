/**
 * 
 */
package edu.ucsd.forward.query.function.math;

import java.math.BigDecimal;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.DecimalType;
import edu.ucsd.forward.data.type.DoubleType;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.DecimalValue;
import edu.ucsd.forward.data.value.DoubleValue;
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
 * Returns sqrt(x^2 +y^2) without intermediate overflow or underflow.
 * 
 * @author Yupeng Fu
 * 
 */
public class HypotFunction extends AbstractMathFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(HypotFunction.class);
    
    public static final String  NAME = "Hypot";
    
    /**
     * The signatures of the function.
     * 
     */
    private enum FunctionSignatureName
    {
        HYPOT_INTEGER,
        
        HYPOT_LONG,
        
        HYPOT_FLOAT,
        
        HYPOT_DOUBLE,
        
        HYPOT_DECIMAL;
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return false;
    }
    
    /**
     * The default constructor.
     */
    public HypotFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String left_name = "left";
        String right_name = "right";
        
        signature = new FunctionSignature(FunctionSignatureName.HYPOT_INTEGER.name(), TypeEnum.DOUBLE.get());
        signature.addArgument(left_name, TypeEnum.INTEGER.get());
        signature.addArgument(right_name, TypeEnum.INTEGER.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.HYPOT_LONG.name(), TypeEnum.DOUBLE.get());
        signature.addArgument(left_name, TypeEnum.LONG.get());
        signature.addArgument(right_name, TypeEnum.LONG.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.HYPOT_FLOAT.name(), TypeEnum.DOUBLE.get());
        signature.addArgument(left_name, TypeEnum.FLOAT.get());
        signature.addArgument(right_name, TypeEnum.FLOAT.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.HYPOT_DOUBLE.name(), TypeEnum.DOUBLE.get());
        signature.addArgument(left_name, TypeEnum.DOUBLE.get());
        signature.addArgument(right_name, TypeEnum.DOUBLE.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.HYPOT_DECIMAL.name(), TypeEnum.DECIMAL.get());
        signature.addArgument(left_name, TypeEnum.DECIMAL.get());
        signature.addArgument(right_name, TypeEnum.DECIMAL.get());
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
        List<Value> arg_values = evaluateArgumentsAndMatchFunctionSignature(call, input);
        FunctionSignatureName sig = FunctionSignatureName.valueOf(call.getFunctionSignature().getName());
        Value left_value = arg_values.get(0);
        Value right_value = arg_values.get(1);
        
        // Handle NULL arguments
        if (left_value instanceof NullValue || right_value instanceof NullValue)
        {
            switch (sig)
            {
                case HYPOT_INTEGER:
                    return new BindingValue(new NullValue(DoubleType.class), true);
                case HYPOT_LONG:
                    return new BindingValue(new NullValue(DoubleType.class), true);
                case HYPOT_FLOAT:
                    return new BindingValue(new NullValue(DoubleType.class), true);
                case HYPOT_DOUBLE:
                    return new BindingValue(new NullValue(DoubleType.class), true);
                case HYPOT_DECIMAL:
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
            double v;
            switch (sig)
            {
                case HYPOT_INTEGER:
                    left_decimal = new BigDecimal((Integer) left);
                    right_decimal = new BigDecimal((Integer) right);
                    
                    v = Math.hypot(left_decimal.doubleValue(), right_decimal.doubleValue());
                    result = new DoubleValue(v);
                    
                    return new BindingValue(result, true);
                case HYPOT_FLOAT:
                    left_decimal = new BigDecimal((Long) left);
                    right_decimal = new BigDecimal((Long) right);
                    
                    v = Math.hypot(left_decimal.doubleValue(), right_decimal.doubleValue());
                    result = new DoubleValue(v);
                    
                    return new BindingValue(result, true);
                case HYPOT_LONG:
                    left_decimal = new BigDecimal((Float) left);
                    right_decimal = new BigDecimal((Float) right);
                    
                    v = Math.hypot(left_decimal.doubleValue(), right_decimal.doubleValue());
                    result = new DoubleValue(v);
                    
                    return new BindingValue(result, true);
                case HYPOT_DOUBLE:
                    left_decimal = new BigDecimal((Double) left);
                    right_decimal = new BigDecimal((Double) right);
                    
                    v = Math.hypot(left_decimal.doubleValue(), right_decimal.doubleValue());
                    result = new DoubleValue(v);
                    
                    return new BindingValue(result, true);
                case HYPOT_DECIMAL:
                    left_decimal = (BigDecimal) left;
                    right_decimal = (BigDecimal) right;
                    
                    v = Math.hypot(left_decimal.doubleValue(), right_decimal.doubleValue());
                    
                    return new BindingValue(new DecimalValue(new BigDecimal((Double) v)), true);
                default:
                    throw new AssertionError();
            }
        }
        catch (ArithmeticException e)
        {
            throw new QueryExecutionException(QueryExecution.ZERO_DIVISOR_DIV);
        }
    }
}
