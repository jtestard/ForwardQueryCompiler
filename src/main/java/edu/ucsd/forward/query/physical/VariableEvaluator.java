package edu.ucsd.forward.query.physical;

import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.QueryProcessor.DataSourceAccess;
import edu.ucsd.forward.query.logical.term.AbsoluteVariable;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Variable;

/**
 * Evaluates a variable given a value.
 * 
 * @author Michalis Petropoulos
 * 
 */
public final class VariableEvaluator
{
    /**
     * Inaccessible constructor.
     */
    private VariableEvaluator()
    {
    }
    
    /**
     * Evaluates a variable and returns an output binding value.
     * 
     * @param variable
     *            the variable to evaluate.
     * @param binding
     *            the binding used by the evaluator.
     * @return The resulting binding value of the evaluator.
     * @throws QueryExecutionException
     *             if an exception is raised during query execution.
     */
    public static BindingValue evaluate(Variable variable, Binding binding) throws QueryExecutionException
    {
        BindingValue output = null;
        if (variable instanceof RelativeVariable)
        {
            // Retrieve the actual value using the query path index position
            output = binding.getValue(variable.getBindingIndex());
        }
        else
        {
            AbsoluteVariable absolute_var = (AbsoluteVariable) variable;
            String source_name = absolute_var.getDataSourceName();
            String data_obj_name = absolute_var.getSchemaObjectName();
            
            // Retrieve the data source access
            DataSourceAccess access = QueryProcessorFactory.getInstance().getDataSourceAccess(source_name);
            
            DataTree data_tree = access.getDataSource().getDataObject(data_obj_name, access.getTransaction());
            if (data_tree != null)
            {
                output = new BindingValue(data_tree.getRootValue(), false);
            }
            else
            {
                output = new BindingValue(new NullValue(variable.getType().getClass()), true);
            }
        }
        
        return output;
    }
    
}
