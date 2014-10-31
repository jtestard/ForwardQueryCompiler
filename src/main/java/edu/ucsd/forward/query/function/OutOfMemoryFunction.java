/**
 * 
 */
package edu.ucsd.forward.query.function;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.general.GeneralFunction;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * A function that throws an out of memory error.
 * 
 * @author Erick Zamora
 * 
 */
public class OutOfMemoryFunction extends AbstractFunction implements GeneralFunction
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(OutOfMemoryFunction.class);
    
    public static final String NAME = "out_of_memory_error";
    
    /**
     * Constructs the function.
     */
    public OutOfMemoryFunction()
    {
        super(NAME, FunctionDefinitionManager.getOutOfMemoryFunction());
    }
    
    /*
     * =============================================================================================================================
     * Overridden methods (GeneralFunctionEvaluator)
     * =============================================================================================================================
     */

    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        throw new OutOfMemoryError();
    }
    
    /*
     * =============================================================================================================================
     * Overridden methods (SqlCompliance)
     * =============================================================================================================================
     */

    @Override
    public boolean isSqlCompliant()
    {
        return false;
    }
}
