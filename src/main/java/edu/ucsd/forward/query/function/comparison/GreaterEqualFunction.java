/**
 * 
 */
package edu.ucsd.forward.query.function.comparison;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.PrimitiveValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.Function;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * The greater than or equal function.
 * 
 * @author Yupeng
 * 
 */
public class GreaterEqualFunction extends AbstractComparisonFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(GreaterEqualFunction.class);
    
    public static final String  NAME = ">=";
    
    /**
     * The signatures of the function.
     * 
     * @author Michalis Petropoulos
     */
    private enum FunctionSignatureName
    {
        GT_EQUAL_INTEGER,

        GT_EQUAL_LONG,

        GT_EQUAL_FLOAT,

        GT_EQUAL_DOUBLE,

        GT_EQUAL_DECIMAL,

        GT_EQUAL_DATE,

        GT_EQUAL_TIMESTAMP,

        GT_EQUAL_BOOLEAN,

        GT_EQUAL_STRING;
    }
    
    /**
     * The default constructor.
     */
    public GreaterEqualFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String left_name = "left";
        String right_name = "right";
        
        signature = new FunctionSignature(FunctionSignatureName.GT_EQUAL_INTEGER.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(left_name, TypeEnum.INTEGER.get());
        signature.addArgument(right_name, TypeEnum.INTEGER.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.GT_EQUAL_LONG.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(left_name, TypeEnum.LONG.get());
        signature.addArgument(right_name, TypeEnum.LONG.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.GT_EQUAL_FLOAT.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(left_name, TypeEnum.FLOAT.get());
        signature.addArgument(right_name, TypeEnum.FLOAT.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.GT_EQUAL_DOUBLE.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(left_name, TypeEnum.DOUBLE.get());
        signature.addArgument(right_name, TypeEnum.DOUBLE.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.GT_EQUAL_DECIMAL.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(left_name, TypeEnum.DECIMAL.get());
        signature.addArgument(right_name, TypeEnum.DECIMAL.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.GT_EQUAL_DATE.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(left_name, TypeEnum.DATE.get());
        signature.addArgument(right_name, TypeEnum.DATE.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.GT_EQUAL_TIMESTAMP.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(left_name, TypeEnum.TIMESTAMP.get());
        signature.addArgument(right_name, TypeEnum.TIMESTAMP.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.GT_EQUAL_BOOLEAN.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(left_name, TypeEnum.BOOLEAN.get());
        signature.addArgument(right_name, TypeEnum.BOOLEAN.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.GT_EQUAL_STRING.name(), TypeEnum.BOOLEAN.get());
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
        
        if (!(left instanceof PrimitiveValue) || !(right instanceof PrimitiveValue))
        {
            throw new UnsupportedOperationException();
        }
        int compare_result = ((PrimitiveValue) left).getObject().compareTo(((PrimitiveValue) right).getObject());
        return new BindingValue(new BooleanValue(compare_result >= 0), true);
    }
    
}
