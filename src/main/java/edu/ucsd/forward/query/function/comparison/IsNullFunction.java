/**
 * 
 */
package edu.ucsd.forward.query.function.comparison;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.Function;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * The IS NULL function.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public class IsNullFunction extends AbstractComparisonFunction implements ComparisonFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(IsNullFunction.class);
    
    public static final String  NAME = "IS NULL";
    
    /**
     * The signatures of the function.
     * 
     * @author Michalis Petropoulos
     */
    private enum FunctionSignatureName
    {
        IS_NULL_COLLECTION,

        IS_NULL_TUPLE,

        IS_NULL_INTEGER,

        IS_NULL_LONG,

        IS_NULL_FLOAT,

        IS_NULL_DOUBLE,

        IS_NULL_DECIMAL,

        IS_NULL_DATE,

        IS_NULL_TIMESTAMP,

        IS_NULL_BOOLEAN,

        IS_NULL_XHTML,

        IS_NULL_STRING;
    }
    
    /**
     * The default constructor.
     */
    public IsNullFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String arg_name = "value";
        
        signature = new FunctionSignature(FunctionSignatureName.IS_NULL_COLLECTION.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(arg_name, TypeEnum.COLLECTION.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.IS_NULL_TUPLE.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(arg_name, TypeEnum.TUPLE.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.IS_NULL_INTEGER.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(arg_name, TypeEnum.INTEGER.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.IS_NULL_LONG.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(arg_name, TypeEnum.LONG.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.IS_NULL_FLOAT.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(arg_name, TypeEnum.FLOAT.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.IS_NULL_DOUBLE.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(arg_name, TypeEnum.DOUBLE.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.IS_NULL_DECIMAL.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(arg_name, TypeEnum.DECIMAL.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.IS_NULL_DATE.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(arg_name, TypeEnum.DATE.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.IS_NULL_TIMESTAMP.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(arg_name, TypeEnum.TIMESTAMP.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.IS_NULL_BOOLEAN.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(arg_name, TypeEnum.BOOLEAN.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.IS_NULL_XHTML.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(arg_name, TypeEnum.XHTML.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.IS_NULL_STRING.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(arg_name, TypeEnum.STRING.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public Notation getNotation()
    {
        return Function.Notation.POSTFIX;
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        Value value = evaluateArgumentsAndMatchFunctionSignature(call, input).get(0);
        return new BindingValue(new BooleanValue(value instanceof NullValue), true);
    }
    
}
