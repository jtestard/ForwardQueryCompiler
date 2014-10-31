/**
 * 
 */
package edu.ucsd.forward.query.function.conditional;

import java.util.Iterator;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.AbstractFunction;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.TermEvaluator;

/**
 * The case function.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class CaseFunction extends AbstractFunction implements CaseFunctionEvaluator
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(CaseFunction.class);
    
    public static final String  NAME = "CASE";
    
    /**
     * The default constructor.
     */
    public CaseFunction()
    {
        super(NAME);
    }
    
    @Override
    public Notation getNotation()
    {
        return Notation.OTHER;
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return true;
    }
    
    @Override
    public BindingValue evaluate(CaseFunctionCall call, Binding input) throws QueryExecutionException
    {
        Iterator<Term> iter_args = call.getArguments().iterator();
        while (iter_args.hasNext())
        {
            Term when = iter_args.next();
            // Evaluate the when term
            Value when_value = TermEvaluator.evaluate(when, input).getSqlValue(TypeEnum.BOOLEAN.get());
            
            Term then = iter_args.next();
            
            if (when_value instanceof NullValue) continue;
            
            if (((BooleanValue) when_value).getObject())
            {
                // Evaluate the then term
                return TermEvaluator.evaluate(then, input);
            }
        }
        
        // Get the type class of the first then term
        Class<? extends Type> type_class = call.getArguments().get(1).getType().getClass();
        
        return new BindingValue(new NullValue(type_class), true);
    }
    
}
