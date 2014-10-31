/**
 * 
 */
package edu.ucsd.forward.query.function.aggregate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsd.app2you.util.StringUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.ast.SetQuantifier;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.TermEvaluator;

/**
 * The string_agg function concatenates the input values into a string, separated by delimiter.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class StringAggFunction extends AbstractAggregateFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(StringAggFunction.class);
    
    public static final String  NAME = "STRING_AGG";
    
    /**
     * The function signatures.
     * 
     * @author Michalis Petropoulos
     */
    private enum FunctionSignatureName
    {
        STRING_AGG_STRING;
    }
    
    /**
     * Default constructor.
     */
    public StringAggFunction()
    {
        super(NAME);
        m_data_source = DataSource.MEDIATOR;
        
        FunctionSignature signature;
        
        String arg1_name = "value";
        String arg2_name = "delimiter";
        
        signature = new FunctionSignature(FunctionSignatureName.STRING_AGG_STRING.name(), TypeEnum.STRING.get());
        signature.addArgument(arg1_name, TypeEnum.STRING.get());
        signature.addArgument(arg2_name, TypeEnum.STRING.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public BindingValue evaluate(AggregateFunctionCall call, Collection<Binding> input, SetQuantifier set_quantifier)
            throws QueryExecutionException
    {
        List<String> strings = new ArrayList<String>();
        
        Set<BindingValue> seen_value = new HashSet<BindingValue>();
        
        Binding in = null;
        
        for (Binding in_binding : input)
        {
            if (in == null) in = in_binding;
            
            // Create the collection of strings
            BindingValue binding = TermEvaluator.evaluate(call.getArguments().get(0), in_binding);
            if (set_quantifier == SetQuantifier.DISTINCT)
            {
                if (seen_value.contains(binding))
                // Have seen the value
                continue;
                else
                // Record it as seen
                seen_value.add(binding);
            }
            
            if (binding.getValue() instanceof NullValue)
            {
                strings.add("");
            }
            else
            {
                strings.add(((StringValue) binding.getValue()).getObject());
            }
        }
        
        // Compute the delimiter
        String delimiter = null;
        if (in != null) delimiter = ((StringValue) TermEvaluator.evaluate(call.getArguments().get(1), in).getValue()).getObject();
        
        if (strings.size() == 0)
        {
            return new BindingValue(new NullValue(), true);
        }
        
        return new BindingValue(new StringValue(StringUtil.join(strings, delimiter)), true);
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return false;
    }
}
