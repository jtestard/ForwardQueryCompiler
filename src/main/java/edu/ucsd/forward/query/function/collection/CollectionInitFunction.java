/**
 * 
 */
package edu.ucsd.forward.query.function.collection;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.AbstractFunction;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunction;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * The function that returns an empty collection if the value is null.
 * 
 * @author Yupeng Fu
 * 
 */
public class CollectionInitFunction extends AbstractFunction implements GeneralFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(CollectionInitFunction.class);
    
    public static final String  NAME = "COLLECTION_INIT";
    
    public CollectionInitFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        signature = new FunctionSignature(NAME, TypeEnum.COLLECTION.get());
        signature.addArgument("collection", TypeEnum.COLLECTION.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return false;
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        List<Value> arg_values = evaluateArgumentsAndMatchFunctionSignature(call, input);
        Value value = arg_values.get(0);
        if (value instanceof NullValue)
        {
            return new BindingValue(new CollectionValue(), true);
        }
        return new BindingValue(value, false);
    }
}
