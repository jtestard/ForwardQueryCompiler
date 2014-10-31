/**
 * 
 */
package edu.ucsd.forward.query.function.string;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.DateValue;
import edu.ucsd.forward.data.value.DecimalValue;
import edu.ucsd.forward.data.value.DoubleValue;
import edu.ucsd.forward.data.value.FloatValue;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.TimestampValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * String formatting function. The timestamp format pattern is documented at
 * http://docs.oracle.com/javase/1.4.2/docs/api/java/text/SimpleDateFormat.html. And the numeric format is documented at
 * http://docs.oracle.com/javase/6/docs/api/java/text/DecimalFormat.html.
 * 
 * @author Yupeng
 * 
 */
public class FormatFunction extends AbstractStringFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(FormatFunction.class);
    
    public static final String  NAME = "format";
    
    /**
     * The signatures of the function.
     * 
     * @author Yupeng
     */
    private enum FunctionSignatureName
    {
        DATE_TO_STRING, TIMESTAMP_TO_STRING, INTEGER_TO_STRING, DOUBLE_TO_STRING, FLOAT_TO_STRING, DECIMAL_TO_STRING;
    }
    
    /**
     * The default constructor.
     */
    public FormatFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String left_name = "value";
        String right_name = "format";
        
        signature = new FunctionSignature(FunctionSignatureName.DATE_TO_STRING.name(), TypeEnum.STRING.get());
        signature.addArgument(left_name, TypeEnum.DATE.get());
        signature.addArgument(right_name, TypeEnum.STRING.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.TIMESTAMP_TO_STRING.name(), TypeEnum.STRING.get());
        signature.addArgument(left_name, TypeEnum.TIMESTAMP.get());
        signature.addArgument(right_name, TypeEnum.STRING.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.INTEGER_TO_STRING.name(), TypeEnum.STRING.get());
        signature.addArgument(left_name, TypeEnum.INTEGER.get());
        signature.addArgument(right_name, TypeEnum.STRING.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.DOUBLE_TO_STRING.name(), TypeEnum.STRING.get());
        signature.addArgument(left_name, TypeEnum.DOUBLE.get());
        signature.addArgument(right_name, TypeEnum.STRING.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.FLOAT_TO_STRING.name(), TypeEnum.STRING.get());
        signature.addArgument(left_name, TypeEnum.FLOAT.get());
        signature.addArgument(right_name, TypeEnum.STRING.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.DECIMAL_TO_STRING.name(), TypeEnum.STRING.get());
        signature.addArgument(left_name, TypeEnum.DECIMAL.get());
        signature.addArgument(right_name, TypeEnum.STRING.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        List<Value> arg_values = evaluateArgumentsAndMatchFunctionSignature(call, input);
        Value left_value = arg_values.get(0);
        Value right_value = arg_values.get(1);
        
        FunctionSignatureName sig = FunctionSignatureName.valueOf(call.getFunctionSignature().getName());
        
        // Handle NULL arguments
        if (left_value instanceof NullValue || right_value instanceof NullValue)
        {
            return new BindingValue(new NullValue(StringType.class), true);
        }
        
        String format = ((StringValue) right_value).toString();
        String result = null;
        switch (sig)
        {
            case DATE_TO_STRING:
                DateValue date_value = (DateValue) left_value;
                result = new SimpleDateFormat(format).format(date_value.getObject());
                break;
            case TIMESTAMP_TO_STRING:
                TimestampValue time_value = (TimestampValue) left_value;
                result = new SimpleDateFormat(format).format(time_value.getObject());
                break;
            case INTEGER_TO_STRING:
                IntegerValue int_value = (IntegerValue) left_value;
                result = new DecimalFormat(format).format(int_value.getObject());
                break;
            case DOUBLE_TO_STRING:
                DoubleValue double_value = (DoubleValue) left_value;
                result = new DecimalFormat(format).format(double_value.getObject());
                break;
            case FLOAT_TO_STRING:
                FloatValue float_value = (FloatValue) left_value;
                result = new DecimalFormat(format).format(float_value.getObject());
                break;
            case DECIMAL_TO_STRING:
                DecimalValue decimal_value = (DecimalValue) left_value;
                result = new DecimalFormat(format).format(decimal_value.getObject());
                break;
            default:
                throw new AssertionError();
        }
        return new BindingValue(new StringValue(result), true);
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        // FIXME the format here different from the to_char function in PostgresQL and format function in MySQL
        return false;
    }
}
