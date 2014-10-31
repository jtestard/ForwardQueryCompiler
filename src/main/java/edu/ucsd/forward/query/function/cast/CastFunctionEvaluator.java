package edu.ucsd.forward.query.function.cast;

import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.FunctionEvaluator;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * Represents a function evaluator that evaluates a cast function given a single input binding that provides values for the input
 * parameters of the function, and outputs a scalar value. The function call can contain nested function calls.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface CastFunctionEvaluator extends FunctionEvaluator
{
    /**
     * Evaluates a cast function call by instantiating its parameters using the input value and returns an output value.
     * 
     * @param call
     *            the cast function call to evaluate.
     * @param input
     *            the input binding used by the evaluator to instantiate function parameters.
     * @return The output value of the function call.
     * @throws QueryExecutionException
     *             if an exception is raised during function evaluation.
     */
    public BindingValue evaluate(CastFunctionCall call, Binding input) throws QueryExecutionException;
    
}
