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
 * The average function that outputs the average (arithmetic mean) of the values of the argument term evaluated across all the input
 * bindings.
 * 
 * @author Yupeng
 * 
 */
public class AverageFunction extends AbstractAggregateFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(AverageFunction.class);
    
    public static final String  NAME = "AVG";
    
    /**
     * The signatures of the function.
     * 
     * @author Michalis Petropoulos
     */
    private enum FunctionSignatureName
    {
        AVG_INTEGER,

        AVG_LONG,

        AVG_FLOAT,

        AVG_DOUBLE,

        AVG_DECIMAL;
    }
    
    /**
     * The default constructor.
     */
    public AverageFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String arg_name = "value";
        
        signature = new FunctionSignature(FunctionSignatureName.AVG_INTEGER.name(), TypeEnum.DECIMAL.get());
        signature.addArgument(arg_name, TypeEnum.INTEGER.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.AVG_LONG.name(), TypeEnum.DECIMAL.get());
        signature.addArgument(arg_name, TypeEnum.LONG.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.AVG_FLOAT.name(), TypeEnum.DOUBLE.get());
        signature.addArgument(arg_name, TypeEnum.FLOAT.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.AVG_DOUBLE.name(), TypeEnum.DOUBLE.get());
        signature.addArgument(arg_name, TypeEnum.DOUBLE.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.AVG_DECIMAL.name(), TypeEnum.DECIMAL.get());
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
                case AVG_INTEGER:
                case AVG_LONG:
                case AVG_DECIMAL:
                    return new BindingValue(new NullValue(DecimalType.class), true);
                case AVG_FLOAT:
                case AVG_DOUBLE:
                    return new BindingValue(new NullValue(DoubleType.class), true);
                default:
                    throw new AssertionError();
            }
        }
        
        Term term = call.getArguments().get(0);
        BigDecimal sum = null;
        int count = 0;
        
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
                    case AVG_INTEGER:
                        sum = sum.add(new BigDecimal((Integer) value.getObject()));
                        break;
                    case AVG_LONG:
                        sum = sum.add(new BigDecimal((Long) value.getObject()));
                        break;
                    case AVG_FLOAT:
                        sum = sum.add(new BigDecimal((Float) value.getObject()));
                        break;
                    case AVG_DOUBLE:
                        sum = sum.add(new BigDecimal((Double) value.getObject()));
                        break;
                    case AVG_DECIMAL:
                        sum = sum.add((BigDecimal) value.getObject());
                        break;
                    default:
                        throw new AssertionError();
                }
                count++;
            }
        }
        
        if (sum == null)
        {
            switch (sig)
            {
                case AVG_INTEGER:
                case AVG_LONG:
                case AVG_DECIMAL:
                    return new BindingValue(new NullValue(DecimalType.class), true);
                case AVG_FLOAT:
                case AVG_DOUBLE:
                    return new BindingValue(new NullValue(DoubleType.class), true);
                default:
                    throw new AssertionError();
            }
        }
        
        BigDecimal avg = sum.divide(new BigDecimal(count));
        
        Value result = new DecimalValue(avg);
        
        try
        {
            switch (sig)
            {
                case AVG_INTEGER:
                case AVG_LONG:
                case AVG_DECIMAL:
                    return new BindingValue(result, true);
                case AVG_FLOAT:
                case AVG_DOUBLE:
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
