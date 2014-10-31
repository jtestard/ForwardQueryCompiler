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
 * The min function that outputs the minimum value of the argument term evaluated across all the input bindings.
 * 
 * @author Yupeng
 * 
 */
public class MinFunction extends AbstractAggregateFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(MinFunction.class);
    
    public static final String  NAME = "MIN";
    
    /**
     * The signatures of the function.
     * 
     * @author Michalis Petropoulos
     */
    private enum FunctionSignatureName
    {
        MIN_INTEGER,

        MIN_LONG,

        MIN_FLOAT,

        MIN_DOUBLE,

        MIN_DECIMAL,

        MIN_DATE,

        MIN_TIMESTAMP,

        MIN_BOOLEAN,

        MIN_STRING;
    }
    
    /**
     * The default constructor.
     */
    public MinFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String arg_name = "value";
        
        signature = new FunctionSignature(FunctionSignatureName.MIN_INTEGER.name(), TypeEnum.INTEGER.get());
        signature.addArgument(arg_name, TypeEnum.INTEGER.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MIN_LONG.name(), TypeEnum.LONG.get());
        signature.addArgument(arg_name, TypeEnum.LONG.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MIN_FLOAT.name(), TypeEnum.FLOAT.get());
        signature.addArgument(arg_name, TypeEnum.FLOAT.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MIN_DOUBLE.name(), TypeEnum.DOUBLE.get());
        signature.addArgument(arg_name, TypeEnum.DOUBLE.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MIN_DECIMAL.name(), TypeEnum.DECIMAL.get());
        signature.addArgument(arg_name, TypeEnum.DECIMAL.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MIN_DATE.name(), TypeEnum.DATE.get());
        signature.addArgument(arg_name, TypeEnum.DATE.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MIN_TIMESTAMP.name(), TypeEnum.TIMESTAMP.get());
        signature.addArgument(arg_name, TypeEnum.TIMESTAMP.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MIN_BOOLEAN.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(arg_name, TypeEnum.BOOLEAN.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.MIN_STRING.name(), TypeEnum.STRING.get());
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
                case MIN_INTEGER:
                    return new BindingValue(new NullValue(IntegerType.class), true);
                case MIN_LONG:
                    return new BindingValue(new NullValue(LongType.class), true);
                case MIN_DECIMAL:
                    return new BindingValue(new NullValue(DecimalType.class), true);
                case MIN_FLOAT:
                    return new BindingValue(new NullValue(FloatType.class), true);
                case MIN_DOUBLE:
                    return new BindingValue(new NullValue(DoubleType.class), true);
                case MIN_DATE:
                    return new BindingValue(new NullValue(DateType.class), true);
                case MIN_TIMESTAMP:
                    return new BindingValue(new NullValue(TimestampType.class), true);
                case MIN_BOOLEAN:
                    return new BindingValue(new NullValue(BooleanType.class), true);
                case MIN_STRING:
                    return new BindingValue(new NullValue(StringType.class), true);
                default:
                    throw new AssertionError();
            }
        }
        
        Term term = call.getArguments().get(0);
        BindingValue min = null;
        for (Binding in_binding : input)
        {
            BindingValue in_value = TermEvaluator.evaluate(term, in_binding);
            if (!(in_value.getValue() instanceof NullValue))
            {
                if (min == null) min = in_value;
                else if (min.compareTo(in_value) > 0) min = in_value;
            }
        }
        
        if (min == null)
        {
            switch (sig)
            {
                case MIN_INTEGER:
                    return new BindingValue(new NullValue(IntegerType.class), true);
                case MIN_LONG:
                    return new BindingValue(new NullValue(LongType.class), true);
                case MIN_DECIMAL:
                    return new BindingValue(new NullValue(DecimalType.class), true);
                case MIN_FLOAT:
                    return new BindingValue(new NullValue(FloatType.class), true);
                case MIN_DOUBLE:
                    return new BindingValue(new NullValue(DoubleType.class), true);
                case MIN_DATE:
                    return new BindingValue(new NullValue(DateType.class), true);
                case MIN_TIMESTAMP:
                    return new BindingValue(new NullValue(TimestampType.class), true);
                case MIN_BOOLEAN:
                    return new BindingValue(new NullValue(BooleanType.class), true);
                case MIN_STRING:
                    return new BindingValue(new NullValue(StringType.class), true);
                default:
                    throw new AssertionError();
            }
        }
        else
        {
            return min;
        }
    }
}
