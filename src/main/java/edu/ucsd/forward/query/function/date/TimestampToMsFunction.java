/**
 * 
 */
package edu.ucsd.forward.query.function.date;

import java.sql.Timestamp;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.LongValue;
import edu.ucsd.forward.data.value.TimestampValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.AbstractFunction;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunction;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * Converts a timestamp into milliseconds.
 * 
 * @author Erick Zamora
 * 
 */
public class TimestampToMsFunction extends AbstractFunction implements GeneralFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(TimestampToMsFunction.class);
    
    public static final String  NAME = "timestamp_to_ms";
    
    /**
     * The signatures of the function.
     * 
     * @author Erick Zamora
     */
    private enum FunctionSignatureName
    {
        TIMESTAMP_TO_MS;
    }
    
    /**
     * The default constructor.
     */
    public TimestampToMsFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String arg1_name = "timestamp";
        signature = new FunctionSignature(FunctionSignatureName.TIMESTAMP_TO_MS.name(), TypeEnum.LONG.get());
        signature.addArgument(arg1_name, TypeEnum.TIMESTAMP.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        Value timestamp_value = evaluateArgumentsAndMatchFunctionSignature(call, input).get(0);
        Timestamp timestamp = ((TimestampValue)timestamp_value).getObject();
        
        return new BindingValue(new LongValue(timestamp.getTime()), false);
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return false;
    }
    
}
