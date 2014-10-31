/**
 * 
 */
package edu.ucsd.forward.query.function.logical;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.TermEvaluator;

/**
 * The OR function.
 * 
 * @author Yupeng
 * 
 */
public class OrFunction extends AbstractLogicalFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(OrFunction.class);
    
    public static final String  NAME = "OR";
    
    /**
     * The signatures of the function.
     * 
     * @author Michalis Petropoulos
     */
    private enum FunctionSignatureName
    {
        OR_BOOLEAN;
    }
    
    /**
     * The default constructor.
     */
    public OrFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String left_name = "left";
        String right_name = "right";
        
        signature = new FunctionSignature(FunctionSignatureName.OR_BOOLEAN.name(), TypeEnum.BOOLEAN.get());
        signature.addArgument(left_name, TypeEnum.BOOLEAN.get());
        signature.addArgument(right_name, TypeEnum.BOOLEAN.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        Term left_term = call.getArguments().get(0);
        Term right_term = call.getArguments().get(1);
        BindingValue left = TermEvaluator.evaluate(left_term, input);
        
        // Short-circuit
        BooleanValue left_v = null;
        if (!(left.getValue() instanceof NullValue))
        {
            left_v = (BooleanValue) left.getSqlValue(TypeEnum.BOOLEAN.get());
            if (left_v.getObject())
            {
                return new BindingValue(new BooleanValue(true), true);
            }
        }
        
        BindingValue right = TermEvaluator.evaluate(right_term, input);
        BooleanValue right_v = null;
        if (!(right.getValue() instanceof NullValue))
        {
            right_v = (BooleanValue) right.getSqlValue(TypeEnum.BOOLEAN.get());
        }
        
        // Handle NULL arguments
        if (left.getValue() instanceof NullValue || right.getValue() instanceof NullValue)
        {
            if (isTrue(left_v) || isTrue(right_v))
            {
                return new BindingValue(new BooleanValue(true), true);
            }
            return new BindingValue(new NullValue(BooleanType.class), true);
        }
        
        boolean result = left_v.getObject() || right_v.getObject();
        
        return new BindingValue(new BooleanValue(result), true);
    }
    
    /**
     * Determines if a value is true.
     * 
     * @param value
     *            the value to check.
     * @return true, if the value is true; otherwise, false.
     */
    private boolean isTrue(Value value)
    {
        if (value == null) return false;
        return ((BooleanValue) value).getObject();
    }
    
}
