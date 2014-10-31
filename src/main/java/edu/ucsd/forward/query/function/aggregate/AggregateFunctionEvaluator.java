package edu.ucsd.forward.query.function.aggregate;

import java.util.Collection;

import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.ast.SetQuantifier;
import edu.ucsd.forward.query.function.FunctionEvaluator;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * Represents a function evaluator that evaluates a function given an input collection of bindings, and outputs a scalar value. The
 * function call can contain nested function calls.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface AggregateFunctionEvaluator extends FunctionEvaluator
{
    /**
     * Evaluates an aggregate function call given an input collection of bindings and returns a scalar value.
     * 
     * @param call
     *            the aggregate function call to evaluate.
     * @param input
     *            the input collection of bindings used by the evaluator.
     * @param set_quantifier
     *            the set quantifier to be used by the evaluator.
     * @return The output value of the function call.
     * @throws QueryExecutionException
     *             if an exception is raised during function evaluation.
     */
    public BindingValue evaluate(AggregateFunctionCall call, Collection<Binding> input, SetQuantifier set_quantifier)
            throws QueryExecutionException;
    
}
