package edu.ucsd.forward.query.function.collection;

import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.FunctionEvaluator;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * Represents a function evaluator that evaluates a collection function given a single input binding that provides values for the
 * input parameters of the function, and outputs a collection value. The function call can contain nested function calls.
 * 
 * @author Romain Vernoux
 * 
 */
public interface CollectionFunctionEvaluator extends FunctionEvaluator
{
    /**
     * Evaluates a collection function call by instantiating its parameters using the input value and returns an output value.
     * 
     * @param call
     *            the collection function call to evaluate.
     * @param input
     *            the input binding used by the evaluator to instantiate function parameters.
     * @return The output value of the function call.
     * @throws QueryExecutionException
     *             if an exception is raised during function evaluation.
     */
    public BindingValue evaluate(CollectionFunctionCall call, Binding input) throws QueryExecutionException;
    
}
