package edu.ucsd.forward.query.physical;

import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.term.AbsoluteVariable;
import edu.ucsd.forward.query.logical.term.Parameter;

/**
 * Evaluates a parameter.
 * 
 * @author Michalis Petropoulos
 * 
 */
public final class ParameterEvaluator
{
    /**
     * Inaccessible constructor.
     */
    private ParameterEvaluator()
    {
    }
    
    /**
     * Evaluates a parameter and returns an output binding value.
     * 
     * @param param
     *            the parameter to evaluate.
     * @param binding
     *            the binding used by the evaluator.
     * @return The resulting binding value of the evaluator.
     * @throws QueryExecutionException
     *             if an exception is raised during query execution.
     */
    public static BindingValue evaluate(Parameter param, Binding binding) throws QueryExecutionException
    {
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        ParameterInstantiations param_insts = qp.getParameterInstantiations();
        
        switch (param.getInstantiationMethod())
        {
            case ASSIGN:
                BindingValue param_inst = param_insts.getInstantiation(param);
                if (param_inst == null)
                {
                    throw new QueryExecutionException(QueryExecution.PARAMETER_EVAL_ERROR, param.getTerm().toString());
                }
                
                return param_inst;
            case COPY:
                AbsoluteVariable var = new AbsoluteVariable(param.getDataSourceName(), param.getSchemaObjectName());
                return TermEvaluator.evaluate(var, binding);
            default:
                throw new AssertionError();
        }
    }
    
}
