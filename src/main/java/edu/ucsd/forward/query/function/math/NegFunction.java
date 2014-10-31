/**
 * 
 */
package edu.ucsd.forward.query.function.math;

import java.math.BigDecimal;

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
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * The NEG function negates an input argument.
 * 
 * @author Yupeng
 * 
 */
public class NegFunction extends AbstractMathFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(NegFunction.class);
    
    public static final String  NAME = "NEG";
    
    /**
     * The signatures of the function.
     * 
     * @author Michalis Petropoulos
     */
    private enum FunctionSignatureName
    {
        NEG_INTEGER,

        NEG_LONG,

        NEG_FLOAT,

        NEG_DOUBLE,

        NEG_DECIMAL;
    }
    
    /**
     * The default constructor.
     */
    public NegFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String arg_name = "value";
        
        signature = new FunctionSignature(FunctionSignatureName.NEG_INTEGER.name(), TypeEnum.INTEGER.get());
        signature.addArgument(arg_name, TypeEnum.INTEGER.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.NEG_LONG.name(), TypeEnum.LONG.get());
        signature.addArgument(arg_name, TypeEnum.LONG.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.NEG_FLOAT.name(), TypeEnum.FLOAT.get());
        signature.addArgument(arg_name, TypeEnum.FLOAT.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.NEG_DOUBLE.name(), TypeEnum.DOUBLE.get());
        signature.addArgument(arg_name, TypeEnum.DOUBLE.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.NEG_DECIMAL.name(), TypeEnum.DECIMAL.get());
        signature.addArgument(arg_name, TypeEnum.DECIMAL.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        FunctionSignatureName sig = FunctionSignatureName.valueOf(call.getFunctionSignature().getName());
        
        Value value = evaluateArgumentsAndMatchFunctionSignature(call, input).get(0);
        
        // Handle NULL argument
        if (value instanceof NullValue)
        {
            switch (sig)
            {
                case NEG_INTEGER:
                    return new BindingValue(new NullValue(IntegerType.class), true);
                case NEG_LONG:
                    return new BindingValue(new NullValue(LongType.class), true);
                case NEG_FLOAT:
                    return new BindingValue(new NullValue(FloatType.class), true);
                case NEG_DOUBLE:
                    return new BindingValue(new NullValue(DoubleType.class), true);
                case NEG_DECIMAL:
                    return new BindingValue(new NullValue(DecimalType.class), true);
                default:
                    throw new AssertionError();
            }
        }
        
        Number number = (Number) ((PrimitiveValue<?>) value).getObject();
        
        try
        {
            BigDecimal decimal;
            Value result;
            switch (sig)
            {
                case NEG_INTEGER:
                    decimal = new BigDecimal((Integer) number);
                    
                    result = new DecimalValue(decimal.multiply(new BigDecimal("-1")));
                    
                    return new BindingValue(TypeConverter.getInstance().convert(result, TypeEnum.INTEGER.get()), true);
                case NEG_LONG:
                    decimal = new BigDecimal((Long) number);
                    
                    result = new DecimalValue(decimal.multiply(new BigDecimal("-1")));
                    
                    return new BindingValue(TypeConverter.getInstance().convert(result, TypeEnum.LONG.get()), true);
                case NEG_FLOAT:
                    decimal = new BigDecimal((Float) number);
                    
                    result = new DecimalValue(decimal.multiply(new BigDecimal("-1")));
                    
                    return new BindingValue(TypeConverter.getInstance().convert(result, TypeEnum.FLOAT.get()), true);
                case NEG_DOUBLE:
                    decimal = new BigDecimal((Double) number);
                    
                    result = new DecimalValue(decimal.multiply(new BigDecimal("-1")));
                    
                    return new BindingValue(TypeConverter.getInstance().convert(result, TypeEnum.DOUBLE.get()), true);
                case NEG_DECIMAL:
                    decimal = (BigDecimal) number;
                    
                    result = new DecimalValue(decimal.multiply(new BigDecimal("-1")));
                    
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
