/**
 * 
 */
package edu.ucsd.forward.query.function.aggregate;

import java.util.Collection;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.type.DateType;
import edu.ucsd.forward.data.type.DecimalType;
import edu.ucsd.forward.data.type.DoubleType;
import edu.ucsd.forward.data.type.FloatType;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.LongType;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.TimestampType;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.PrimitiveValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.ast.SetQuantifier;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.TermEvaluator;

/**
 * The max function that outputs the maximum value of the argument term evaluated across all the input bindings.
 * 
 * @author Yupeng
 * 
 */
public class MaxFunction extends AbstractAggregateFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(MaxFunction.class);
    
    public static final String  NAME = "MAX";
    
    /**
     * The signatures of the function.
     * 
     * @author Michalis Petropoulos
     */
    private enum FunctionSignatureName
    {
        MAX_INTEGER,

        MAX_LONG,

        MAX_FLOAT,

        MAX_DOUBLE,

        MAX_DECIMAL,

        MAX_DATE,

        MAX_TIMESTAMP,

        MAX_BOOLEAN,

        MAX_STRING;
    }
    
    /**
     * The default constructor.
     */
    public MaxFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String arg_name = "value";
        
        signature = new FunctionSignature(FunctionSignatureName.MAX_INTEGER.name(), TypeEnum.INTEGER.get());
        signature.addArgument(arg_name, TypeEnum.INTEGER.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MAX_LONG.name(), TypeEnum.LONG.get());
        signature.addArgument(arg_name, TypeEnum.LONG.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MAX_FLOAT.name(), TypeEnum.FLOAT.get());
        signature.addArgument(arg_name, TypeEnum.FLOAT.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MAX_DOUBLE.name(), TypeEnum.DOUBLE.get());
        signature.addArgument(arg_name, TypeEnum.DOUBLE.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MAX_DECIMAL.name(), TypeEnum.DECIMAL.get());
        signature.addArgument(arg_name, TypeEnum.DECIMAL.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MAX_DATE.name(), TypeEnum.DATE.get());
        signature.addArgument(arg_name, TypeEnum.DATE.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MAX_TIMESTAMP.name(), TypeEnum.TIMESTAMP.get());
        signature.addArgument(arg_name, TypeEnum.TIMESTAMP.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MAX_BOOLEAN.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(arg_name, TypeEnum.BOOLEAN.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MAX_STRING.name(), TypeEnum.STRING.get());
        signature.addArgument(arg_name, TypeEnum.STRING.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public BindingValue evaluate(AggregateFunctionCall call, Collection<Binding> input, SetQuantifier set_quantifier)
            throws QueryExecutionException
    {
        FunctionSignatureName sig = FunctionSignatureName.valueOf(call.getFunctionSignature().getName());
        
        if (input.isEmpty())
        {
            switch (sig)
            {
                case MAX_INTEGER:
                    return new BindingValue(new NullValue(IntegerType.class), true);
                case MAX_LONG:
                    return new BindingValue(new NullValue(LongType.class), true);
                case MAX_DECIMAL:
                    return new BindingValue(new NullValue(DecimalType.class), true);
                case MAX_FLOAT:
                    return new BindingValue(new NullValue(FloatType.class), true);
                case MAX_DOUBLE:
                    return new BindingValue(new NullValue(DoubleType.class), true);
                case MAX_DATE:
                    return new BindingValue(new NullValue(DateType.class), true);
                case MAX_TIMESTAMP:
                    return new BindingValue(new NullValue(TimestampType.class), true);
                case MAX_BOOLEAN:
                    return new BindingValue(new NullValue(BooleanType.class), true);
                case MAX_STRING:
                    return new BindingValue(new NullValue(StringType.class), true);
                default:
                    throw new AssertionError();
            }
        }
        
        Term term = call.getArguments().get(0);
        BindingValue max = null;
        
        for (Binding in_binding : input)
        {
            BindingValue in_value = TermEvaluator.evaluate(term, in_binding);
            if (!(in_value.getValue() instanceof NullValue))
            {
                if (max == null) max = in_value;
                else if (max.compareTo(in_value) < 0) max = in_value;
            }
        }
        
        if (max == null)
        {
            switch (sig)
            {
                case MAX_INTEGER:
                    return new BindingValue(new NullValue(IntegerType.class), true);
                case MAX_LONG:
                    return new BindingValue(new NullValue(LongType.class), true);
                case MAX_DECIMAL:
                    return new BindingValue(new NullValue(DecimalType.class), true);
                case MAX_FLOAT:
                    return new BindingValue(new NullValue(FloatType.class), true);
                case MAX_DOUBLE:
                    return new BindingValue(new NullValue(DoubleType.class), true);
                case MAX_DATE:
                    return new BindingValue(new NullValue(DateType.class), true);
                case MAX_TIMESTAMP:
                    return new BindingValue(new NullValue(TimestampType.class), true);
                case MAX_BOOLEAN:
                    return new BindingValue(new NullValue(BooleanType.class), true);
                case MAX_STRING:
                    return new BindingValue(new NullValue(StringType.class), true);
                default:
                    throw new AssertionError();
            }
        }
        else
        {
            return max;
        }
    }
}
