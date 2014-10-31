/**
 * 
 */
package edu.ucsd.forward.query.function.aggregate;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.DecimalType;
import edu.ucsd.forward.data.type.DoubleType;
import edu.ucsd.forward.data.type.FloatType;
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
import edu.ucsd.forward.query.ast.SetQuantifier;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.TermEvaluator;

/**
 * The sum function.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public class SumFunction extends AbstractAggregateFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(SumFunction.class);
    
    public static final String  NAME = "SUM";
    
    /**
     * The function signatures.
     * 
     * @author Michalis Petropoulos
     */
    private enum FunctionSignatureName
    {
        SUM_INTEGER,

        SUM_LONG,

        SUM_FLOAT,

        SUM_DOUBLE,

        SUM_DECIMAL;
    }
    
    /**
     * The default constructor.
     */
    public SumFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String arg_name = "value";
        
        signature = new FunctionSignature(FunctionSignatureName.SUM_INTEGER.name(), TypeEnum.LONG.get());
        signature.addArgument(arg_name, TypeEnum.INTEGER.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.SUM_LONG.name(), TypeEnum.DECIMAL.get());
        signature.addArgument(arg_name, TypeEnum.LONG.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.SUM_FLOAT.name(), TypeEnum.FLOAT.get());
        signature.addArgument(arg_name, TypeEnum.FLOAT.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.SUM_DOUBLE.name(), TypeEnum.DOUBLE.get());
        signature.addArgument(arg_name, TypeEnum.DOUBLE.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.SUM_DECIMAL.name(), TypeEnum.DECIMAL.get());
        signature.addArgument(arg_name, TypeEnum.DECIMAL.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public BindingValue evaluate(AggregateFunctionCall call, Collection<Binding> input, SetQuantifier set_quantifier)
            throws QueryExecutionException
    {
        FunctionSignatureName sig = FunctionSignatureName.valueOf(call.getFunctionSignature().getName());
        
        // If the input is empty return null
        if (input.isEmpty())
        {
            switch (sig)
            {
                case SUM_INTEGER:
                    return new BindingValue(new NullValue(LongType.class), true);
                case SUM_LONG:
                case SUM_DECIMAL:
                    return new BindingValue(new NullValue(DecimalType.class), true);
                case SUM_FLOAT:
                    return new BindingValue(new NullValue(FloatType.class), true);
                case SUM_DOUBLE:
                    return new BindingValue(new NullValue(DoubleType.class), true);
                default:
                    throw new AssertionError();
            }
        }
        
        Term term = call.getArguments().get(0);
        BigDecimal sum = null;
        
        Set<BindingValue> seen_value = new HashSet<BindingValue>();
        for (Binding in_binding : input)
        {
            BindingValue in_value = TermEvaluator.evaluate(term, in_binding);
            
            if (set_quantifier == SetQuantifier.DISTINCT)
            {
                if (seen_value.contains(in_value))
                // Have seen the value
                continue;
                else
                // Record it as seen
                seen_value.add(in_value);
            }
            
            if (!(in_value.getValue() instanceof NullValue))
            {
                if (sum == null) sum = new BigDecimal("0");
                FunctionSignature signature = call.getFunctionSignature();
                PrimitiveValue<?> value = (PrimitiveValue<?>) in_value.getSqlValue(signature.getArguments().get(0).getType());
                switch (sig)
                {
                    case SUM_INTEGER:
                        sum = sum.add(new BigDecimal((Integer) value.getObject()));
                        break;
                    case SUM_LONG:
                        sum = sum.add(new BigDecimal((Long) value.getObject()));
                        break;
                    case SUM_FLOAT:
                        sum = sum.add(new BigDecimal((Float) value.getObject()));
                        break;
                    case SUM_DOUBLE:
                        sum = sum.add(new BigDecimal((Double) value.getObject()));
                        break;
                    case SUM_DECIMAL:
                        sum = sum.add((BigDecimal) value.getObject());
                        break;
                    default:
                        throw new AssertionError();
                }
            }
        }
        
        if (sum == null)
        {
            switch (sig)
            {
                case SUM_INTEGER:
                    return new BindingValue(new NullValue(LongType.class), true);
                case SUM_LONG:
                case SUM_DECIMAL:
                    return new BindingValue(new NullValue(DecimalType.class), true);
                case SUM_FLOAT:
                    return new BindingValue(new NullValue(FloatType.class), true);
                case SUM_DOUBLE:
                    return new BindingValue(new NullValue(DoubleType.class), true);
                default:
                    throw new AssertionError();
            }
        }
        
        Value result = new DecimalValue(sum);
        
        try
        {
            switch (sig)
            {
                case SUM_INTEGER:
                    return new BindingValue(TypeConverter.getInstance().convert(result, TypeEnum.LONG.get()), true);
                case SUM_LONG:
                case SUM_DECIMAL:
                    return new BindingValue(result, true);
                case SUM_FLOAT:
                    return new BindingValue(TypeConverter.getInstance().convert(result, TypeEnum.FLOAT.get()), true);
                case SUM_DOUBLE:
                    return new BindingValue(TypeConverter.getInstance().convert(result, TypeEnum.DOUBLE.get()), true);
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
