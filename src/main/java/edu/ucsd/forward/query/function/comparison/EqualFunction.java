/**
 * 
 */
package edu.ucsd.forward.query.function.comparison;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.PrimitiveValue;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.Function;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * The equality function.
 * 
 * @author Yupeng
 * 
 */
public class EqualFunction extends AbstractComparisonFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(EqualFunction.class);
    
    public static final String  NAME = "=";
    
    /**
     * The signatures of the function.
     * 
     * @author Michalis Petropoulos
     */
    private enum FunctionSignatureName
    {
        EQUAL_INTEGER,

        EQUAL_LONG,

        EQUAL_FLOAT,

        EQUAL_DOUBLE,

        EQUAL_DECIMAL,

        EQUAL_DATE,

        EQUAL_TIMESTAMP,

        EQUAL_BOOLEAN,

        EQUAL_XHTML,

        EQUAL_STRING;
    }
    
    /**
     * The default constructor.
     */
    public EqualFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String left_name = "left";
        String right_name = "right";
        
        signature = new FunctionSignature(FunctionSignatureName.EQUAL_INTEGER.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(left_name, TypeEnum.INTEGER.get());
        signature.addArgument(right_name, TypeEnum.INTEGER.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.EQUAL_LONG.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(left_name, TypeEnum.LONG.get());
        signature.addArgument(right_name, TypeEnum.LONG.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.EQUAL_FLOAT.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(left_name, TypeEnum.FLOAT.get());
        signature.addArgument(right_name, TypeEnum.FLOAT.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.EQUAL_DOUBLE.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(left_name, TypeEnum.DOUBLE.get());
        signature.addArgument(right_name, TypeEnum.DOUBLE.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.EQUAL_DECIMAL.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(left_name, TypeEnum.DECIMAL.get());
        signature.addArgument(right_name, TypeEnum.DECIMAL.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.EQUAL_DATE.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(left_name, TypeEnum.DATE.get());
        signature.addArgument(right_name, TypeEnum.DATE.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.EQUAL_TIMESTAMP.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(left_name, TypeEnum.TIMESTAMP.get());
        signature.addArgument(right_name, TypeEnum.TIMESTAMP.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.EQUAL_BOOLEAN.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(left_name, TypeEnum.BOOLEAN.get());
        signature.addArgument(right_name, TypeEnum.BOOLEAN.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.EQUAL_XHTML.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(left_name, TypeEnum.XHTML.get());
        signature.addArgument(right_name, TypeEnum.XHTML.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.EQUAL_STRING.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(left_name, TypeEnum.STRING.get());
        signature.addArgument(right_name, TypeEnum.STRING.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public Notation getNotation()
    {
        return Function.Notation.INFIX;
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        List<Value> arg_values = evaluateArgumentsAndMatchFunctionSignature(call, input);
        Value left = arg_values.get(0);
        Value right = arg_values.get(1);
        
        // Handle NULL arguments
        if (left instanceof NullValue || right instanceof NullValue)
        {
            return new BindingValue(new NullValue(BooleanType.class), true);
        }
        
        boolean result;
        if (left instanceof PrimitiveValue<?>)
        {
            result = ((PrimitiveValue) left).getObject().compareTo(((PrimitiveValue) right).getObject()) == 0;
        }
        else if (left instanceof ScalarValue)
        {
            result = ValueUtil.deepEquals(left, right);
        }
        else
        {
            throw new AssertionError();
        }
        
        return new BindingValue(new BooleanValue(result), true);
    }
}
