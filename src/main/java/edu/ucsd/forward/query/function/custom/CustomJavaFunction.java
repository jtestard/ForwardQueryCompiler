/**
 * 
 */
package edu.ucsd.forward.query.function.custom;

import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.function.string.AbstractStringFunction;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * An example custom Java function returning the string "custom".
 * 
 * @author Michalis Petropoulos
 */
public class CustomJavaFunction extends AbstractStringFunction
{
    public static final String NAME = "custom";

    /**
     * The default constructor.
     */
    public CustomJavaFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        signature = new FunctionSignature("sig", TypeEnum.STRING.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        return new BindingValue(new StringValue("custom"), true);
    }
}
