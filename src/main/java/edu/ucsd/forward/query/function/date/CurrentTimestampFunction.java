/**
 * 
 */
package edu.ucsd.forward.query.function.date;

import java.sql.Timestamp;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.TimestampValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.AbstractFunction;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunction;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * Returns the current time stamp.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class CurrentTimestampFunction extends AbstractFunction implements GeneralFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(CurrentTimestampFunction.class);
    
    public static final String  NAME = "current_timestamp";
    
    /**
     * The signatures of the function.
     * 
     * @author Michalis Petropoulos
     */
    private enum FunctionSignatureName
    {
        CURRENT_TIMESTAMP_NO_ARGS;
    }
    
    /**
     * The default constructor.
     */
    public CurrentTimestampFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        signature = new FunctionSignature(FunctionSignatureName.CURRENT_TIMESTAMP_NO_ARGS.name(), TypeEnum.TIMESTAMP.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        evaluateArgumentsAndMatchFunctionSignature(call, input);
        
        return new BindingValue(new TimestampValue(new Timestamp(System.currentTimeMillis())), true);
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return true;
    }
    
}
