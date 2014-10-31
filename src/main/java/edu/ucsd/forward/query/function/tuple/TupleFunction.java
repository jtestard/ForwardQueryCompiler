/**
 * 
 */
package edu.ucsd.forward.query.function.tuple;

import java.util.Iterator;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.AbstractFunction;
import edu.ucsd.forward.query.logical.term.Constant;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.TermEvaluator;

/**
 * The tuple construction function.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class TupleFunction extends AbstractFunction implements TupleFunctionEvaluator
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(TupleFunction.class);
    
    public static final String  NAME = "TUPLE";
    
    /**
     * The default constructor.
     */
    public TupleFunction()
    {
        super(NAME);
        m_data_source = DataSource.MEDIATOR;
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return false;
    }
    
    @Override
    public BindingValue evaluate(TupleFunctionCall call, Binding input) throws QueryExecutionException
    {
        TupleValue tuple_value = new TupleValue();
        
        Iterator<Term> iter_args = call.getArguments().iterator();
        while (iter_args.hasNext())
        {
            Term arg = iter_args.next();
            BindingValue b_value = TermEvaluator.evaluate(arg, input);
            Value attr_value = b_value.getValue();
            
            if (!b_value.isCloned())
            {
                // Make a copy of the value, because the original value already has a tuple parent.
                attr_value = ValueUtil.cloneNoParentNoType(attr_value);
            }
            
            Constant attr_name_const = (Constant) iter_args.next();
            String attr_name = attr_name_const.getValue().toString();
            
            tuple_value.setAttribute(attr_name, attr_value);
        }
        
        tuple_value.setInline(call.isInline());
        
        return new BindingValue(tuple_value, true);
    }
    
}
