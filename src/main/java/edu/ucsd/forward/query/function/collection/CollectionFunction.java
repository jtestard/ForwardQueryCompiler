/**
 * 
 */
package edu.ucsd.forward.query.function.collection;

import java.util.Iterator;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.AbstractFunction;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.TermEvaluator;

/**
 * The tuple construction function.
 * 
 * @author Romain Vernoux
 * 
 */
public class CollectionFunction extends AbstractFunction implements CollectionFunctionEvaluator
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(CollectionFunction.class);
    
    public static final String  NAME = "COLLECTION";
    
    /**
     * The default constructor.
     */
    public CollectionFunction()
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
    public BindingValue evaluate(CollectionFunctionCall call, Binding input) throws QueryExecutionException
    {
        CollectionValue collection_value = new CollectionValue();
        
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
            
            collection_value.add(attr_value);
        }
        
        return new BindingValue(collection_value, true);
    }
    
}
