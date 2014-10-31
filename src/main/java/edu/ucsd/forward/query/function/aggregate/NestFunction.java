/**
 * 
 */
package edu.ucsd.forward.query.function.aggregate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.ast.SetQuantifier;
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingBuffer;
import edu.ucsd.forward.query.physical.BindingBufferHash;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.TermEvaluator;

/**
 * The nest function that groups the values of the argument terms evaluated across all the input bindings and returns a nested
 * collection.
 * 
 * @author Yupeng
 * 
 */
public class NestFunction extends AbstractAggregateFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(NestFunction.class);
    
    public static final String  NAME = "NEST";
    
    /**
     * Default constructor.
     */
    public NestFunction()
    {
        super(NAME);
        m_data_source = DataSource.MEDIATOR;
    }
    
    @Override
    public BindingValue evaluate(AggregateFunctionCall call, Collection<Binding> input, SetQuantifier set_quantifier)
            throws QueryExecutionException
    {
        // The buffer for the bindings in set semantics.
        BindingBuffer index = null;
        
        if (set_quantifier == SetQuantifier.DISTINCT)
        {
            OutputInfo binding_info = new OutputInfo();
            List<Term> conditions = new ArrayList<Term>();
            
            int i = 0;
            for (Term term : call.getArguments())
            {
                assert (term instanceof RelativeVariable);
                RelativeVariable nest_var = (RelativeVariable) term.copy();
                binding_info.add(nest_var, term);
                nest_var.setBindingIndex(i++);
                conditions.add(nest_var);
            }
            
            index = new BindingBufferHash(conditions, binding_info, false);
        }
        
        CollectionValue result = new CollectionValue();
        
        // Get the output attribute names
        List<String> attr_names = new ArrayList<String>();
        for (Term term : call.getArguments())
        {
            attr_names.add(term.getDefaultProjectAlias());
        }
        
        for (Binding in_binding : input)
        {
            // Create the binding
            Binding out_binding = new Binding();
            for (Term term : call.getArguments())
            {
                // Evaluate the argument
                out_binding.addValue(TermEvaluator.evaluate(term, in_binding));
            }
            
            // Have seen the binding
            if (set_quantifier == SetQuantifier.DISTINCT && index.get(out_binding, false).hasNext()) continue;
            
            if (set_quantifier == SetQuantifier.DISTINCT) index.add(out_binding);
            
            // Construct the tuple
            TupleValue tuple = new TupleValue();
            int count = 0;
            for (int i = 0; i < attr_names.size(); i++)
            {
                Value value = out_binding.getValue(i).getValue();
                // Clone the value if it is not cloned.
                if (!out_binding.getValue(i).isCloned()) value = ValueUtil.cloneNoParentNoType(value);
                tuple.setAttribute(attr_names.get(i), value);
                if (value instanceof NullValue) count++;
            }
            // If all attributes of a tuple are null, the tuple is discarded
            // if(count < tuple.getAttributeNames().size())
            result.add(tuple);
        }
        
        return new BindingValue(result, true);
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return false;
    }
}
