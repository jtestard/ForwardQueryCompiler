package edu.ucsd.forward.query.function.tuple;

import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.FunctionEvaluator;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * Represents a function evaluator that evaluates a tuple function given a single input binding that provides values for the input
 * parameters of the function, and outputs a tuple value. The function call can contain nested function calls.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface TupleFunctionEvaluator extends FunctionEvaluator
{
    /**
     * Evaluates a tuple function call by instantiating its parameters using the input value and returns an output value.
     * 
     * @param call
     *            the tuple function call to evaluate.
     * @param input
     *            the input binding used by the evaluator to instantiate function parameters.
     * @return The output value of the function call.
     * @throws QueryExecutionException
     *             if an exception is raised during function evaluation.
     */
    public BindingValue evaluate(TupleFunctionCall call, Binding input) throws QueryExecutionException;
    
}
