/**
 * 
 */
package edu.ucsd.forward.query.function.string;

import org.apache.commons.lang.StringUtils;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * PostgreSqls split_part function.
 * 
 * FIXME: This string function is "SqlCompliant" only because postgresql supports it. When we start adding support for other
 * functions this will need to be revisited
 * 
 * @author Erick Zamora
 * 
 */
public class SplitPartFunction extends AbstractStringFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(SplitPartFunction.class);
    
    public static final String  NAME = "split_part";
    
    /**
     * The signatures of the function.
     * 
     * @author Erick Zamora
     */
    private enum FunctionSignatureName
    {
        SPLIT_PART;
    }
    
    /**
     * The default constructor.
     */
    public SplitPartFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        String arg1_name = "string";
        String arg2_name = "delimiter";
        String arg3_name = "field";
        
        signature = new FunctionSignature(FunctionSignatureName.SPLIT_PART.name(), TypeEnum.STRING.get());
        signature.addArgument(arg1_name, TypeEnum.STRING.get());
        signature.addArgument(arg2_name, TypeEnum.STRING.get());
        signature.addArgument(arg3_name, TypeEnum.INTEGER.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        Value string_value = evaluateArgumentsAndMatchFunctionSignature(call, input).get(0);
        Value delimiter_value = evaluateArgumentsAndMatchFunctionSignature(call, input).get(1);
        Value field_value = evaluateArgumentsAndMatchFunctionSignature(call, input).get(2);
        
        // Handle NULL arguments
        if (delimiter_value instanceof NullValue || string_value instanceof NullValue || field_value instanceof NullValue)
        {
            return new BindingValue(new NullValue(StringType.class), false);
        }
        String string = ((StringValue) string_value).toString();
        String delimiter = ((StringValue) delimiter_value).toString();
        
        // PostgreSql is 1 indexed, account for that here
        int field = ((IntegerValue) field_value).getObject() - 1;
        
        String[] split_string = StringUtils.splitByWholeSeparatorPreserveAllTokens(string, delimiter);
        
        // Handle index out of bounds
        if (field >= split_string.length)
        {
            return new BindingValue(new StringValue(""), false);
        }
        else if (field < 0)
        {
            throw new QueryExecutionException(QueryExecution.FUNCTION_EVAL_ERROR,
                                              "split_part's field position must be greater than zero");
        }
        
        return new BindingValue(new StringValue(split_string[field]), false);
    }
}
