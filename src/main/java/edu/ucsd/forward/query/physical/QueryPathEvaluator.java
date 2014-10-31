package edu.ucsd.forward.query.physical;

import edu.ucsd.forward.data.json.JsonValueNavigationUtil;
import edu.ucsd.forward.data.type.TypeException;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.JsonValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.SwitchValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.term.QueryPath;

/**
 * Evaluates a query path given a value.
 * 
 * @author Michalis Petropoulos
 * 
 */
public final class QueryPathEvaluator
{
    /**
     * Inaccessible constructor.
     */
    private QueryPathEvaluator()
    {
    }
    
    /**
     * Evaluates a query path and returns an output binding value.
     * 
     * @param query_path
     *            the query path to evaluate.
     * @param binding
     *            the binding used by the evaluator.
     * @return The resulting binding value of the evaluator.
     * @throws QueryExecutionException
     *             if an exception is raised during query execution.
     */
    public static BindingValue evaluate(QueryPath query_path, Binding binding) throws QueryExecutionException
    {
        BindingValue output = null;
        
        Value value = TermEvaluator.evaluate(query_path.getTerm(), binding).getValue();
        Value out_value=evaluateFromValue(query_path, value);
        output = new BindingValue(out_value, false);
        
        return output;
    }
    
    /**
     * Evaluates a query path and returns an output value.
     * 
     * @param query_path
     *            the query path to evaluate.
     * @param in_value
     *            the value.
     * @return The resulting value of the evaluator.
     * @throws QueryExecutionException
     *             if an exception is raised during query execution.
     */
    public static Value evaluateFromValue(QueryPath query_path, Value in_value) throws QueryExecutionException
    {
        Value value = in_value;
        
        for (String step : query_path.getPathSteps())
        {
            if (value instanceof TupleValue)
            {
                value = ((TupleValue) value).getAttribute(step);
            }
            else if (value instanceof CollectionValue)
            {
                CollectionValue collection = (CollectionValue) value;
                assert (collection.getTuples().size() == 1);
                value = collection.getTuples().get(0).getAttribute(step);
            }
            else if (value instanceof SwitchValue)
            {
                if (((SwitchValue) value).getCaseName().equals(step))
                {
                    value = ((SwitchValue) value).getCase();
                }
                else
                {
                    value = null;
                }
            }
            else if (value instanceof JsonValue)
            {
                try
                {
                    value = JsonValueNavigationUtil.navigate((JsonValue) value, step);
                }
                catch (TypeException e)
                {
                    throw new QueryExecutionException(QueryExecution.QUERY_PATH_EVAL_ERROR, e, query_path.toString());
                }
            }
            else
            {
                assert (value instanceof NullValue);
                
                value = new NullValue(query_path.getType().getClass());
            }
            
            if (value == null)
            {
                value = new NullValue(query_path.getType().getClass());
                break;
            }
        }
        
        return value;
    }
}
