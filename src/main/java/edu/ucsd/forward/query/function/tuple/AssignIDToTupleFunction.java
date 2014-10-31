/**
 * 
 */
package edu.ucsd.forward.query.function.tuple;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.AbstractFunction;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.FunctionSignature.Occurrence;
import edu.ucsd.forward.query.function.general.GeneralFunction;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * Adds a unique id to each tuple of a collection
 * 
 * @author Vicky Papavasileiou
 * 
 */
public class AssignIDToTupleFunction extends AbstractFunction implements GeneralFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(AssignIDToTupleFunction.class);
    
    public static final String  NAME = "ASSIGNIDTOTUPLE";
    
    /**
     * The default constructor.
     */
    public AssignIDToTupleFunction()
    {
        super(NAME);
        FunctionSignature signature = new FunctionSignature(NAME, TypeEnum.INTEGER.get());
        signature.addArgument("arg",TypeEnum.STRING.get(),Occurrence.MULTIPLE);
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
        
        IntegerValue result = new IntegerValue(input.hashCode());
        
        return new BindingValue(result, true);
    }
    
}
