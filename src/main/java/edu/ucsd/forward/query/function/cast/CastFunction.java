/**
 * 
 */
package edu.ucsd.forward.query.function.cast;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeConverter;
import edu.ucsd.forward.data.type.TypeException;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.AbstractFunction;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.TermEvaluator;

/**
 * A cast function specifies a conversion from one type to another.
 * 
 * @author Yupeng
 * 
 */
public class CastFunction extends AbstractFunction implements CastFunctionEvaluator
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(CastFunction.class);
    
    public static final String  NAME = "CAST";
    
    /**
     * The default constructor.
     */
    public CastFunction()
    {
        super(NAME);
    }
    
    @Override
    public BindingValue evaluate(CastFunctionCall call, Binding input) throws QueryExecutionException
    {
        Term term = call.getArguments().get(0);
        BindingValue source = TermEvaluator.evaluate(term, input);
        
        Type target_type = call.getTargetType().get();
        
        // Handle NULL arguments
        if (source.getValue() instanceof NullValue)
        {
            return new BindingValue(new NullValue(target_type.getClass()), true);
        }
        
        // Cast only if needed
        if (source.getValue().getTypeClass() != target_type.getClass())
        {
            Value value;
            try
            {
                value = TypeConverter.getInstance().convert(source.getSqlValue(), target_type);
            }
            catch (TypeException e)
            {
                // Chain the exception
                throw new QueryExecutionException(QueryExecution.FUNCTION_EVAL_ERROR, e, this.getName());
            }
            
            return new BindingValue(value, false);
        }
        else
        {
            return source;
        }
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return true;
    }
    
}
