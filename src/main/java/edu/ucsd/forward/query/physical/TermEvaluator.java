package edu.ucsd.forward.query.physical;

import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.cast.CastFunctionCall;
import edu.ucsd.forward.query.function.collection.CollectionFunctionCall;
import edu.ucsd.forward.query.function.conditional.CaseFunctionCall;
import edu.ucsd.forward.query.function.external.ExternalFunctionCall;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.function.tuple.TupleFunctionCall;
import edu.ucsd.forward.query.logical.term.Constant;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;

/**
 * Represents a term evaluator that evaluates either a function call, a query path or a constant. The evaluation takes as input a
 * value that provides input parameters for the function call and the query path. Note that a function call can contain nested
 * function calls.
 * 
 * @author Michalis Petropoulos
 * 
 */
public final class TermEvaluator
{
    /**
     * Inaccessible constructor.
     */
    private TermEvaluator()
    {
    }
    
    /**
     * Evaluates a term that is either a function call, a query path or a constant, and returns an output binding value.
     * 
     * @param term
     *            the term to evaluate.
     * @param binding
     *            the binding used by the evaluator to instantiate input parameters for a function call or a query path.
     * @return The resulting binding value of the evaluator.
     * @throws QueryExecutionException
     *             if an exception is raised during term evaluation.
     */
    public static BindingValue evaluate(Term term, Binding binding) throws QueryExecutionException
    {
        if (term instanceof Constant)
        {
            // The isCloned flag for constants is set to false because they can occur at operators that do not reset the isCloned
            // flag of binding values, such as Project.
            return new BindingValue(((Constant) term).getValue(), false);
        }
        else if (term instanceof Variable)
        {
            return VariableEvaluator.evaluate((Variable) term, binding);
        }
        else if (term instanceof Parameter)
        {
            return ParameterEvaluator.evaluate((Parameter) term, binding);
        }
        else if (term instanceof QueryPath)
        {
            return QueryPathEvaluator.evaluate((QueryPath) term, binding);
        }
        else if (term instanceof GeneralFunctionCall)
        {
            GeneralFunctionCall call = (GeneralFunctionCall) term;
            return call.getFunction().evaluate(call, binding);
        }
        else if (term instanceof ExternalFunctionCall)
        {
            ExternalFunctionCall call = (ExternalFunctionCall) term;
            return call.getFunction().evaluate(call, binding);
        }
        else if (term instanceof CaseFunctionCall)
        {
            CaseFunctionCall call = (CaseFunctionCall) term;
            return call.getFunction().evaluate(call, binding);
        }
        else if (term instanceof CastFunctionCall)
        {
            CastFunctionCall call = (CastFunctionCall) term;
            return call.getFunction().evaluate(call, binding);
        }
        else if (term instanceof TupleFunctionCall)
        {
            TupleFunctionCall call = (TupleFunctionCall) term;
            return call.getFunction().evaluate(call, binding);
        }
        else if (term instanceof CollectionFunctionCall)
        {
            CollectionFunctionCall call = (CollectionFunctionCall) term;
            return call.getFunction().evaluate(call, binding);
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }
}
