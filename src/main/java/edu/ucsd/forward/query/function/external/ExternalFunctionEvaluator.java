package edu.ucsd.forward.query.function.external;

import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.FunctionEvaluator;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * Represents a function evaluator that evaluates an external function given a single input binding that provides values for the
 * input parameters of the function, and outputs a value. The function call can contain nested function calls.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface ExternalFunctionEvaluator extends FunctionEvaluator
{
    /**
     * Evaluates an external function call by instantiating its parameters using the input value and returns an output value.
     * 
     * @param call
     *            the external function call to evaluate.
     * @param input
     *            the input binding used by the evaluator to instantiate function parameters.
     * @return The output value of the function call.
     * @throws QueryExecutionException
     *             if an exception is raised during function evaluation.
     */
    public BindingValue evaluate(ExternalFunctionCall call, Binding input) throws QueryExecutionException;
    
}
