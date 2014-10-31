/**
 * 
 */
package edu.ucsd.forward.query.function.aggregate;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.LongType;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.LongValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.ast.SetQuantifier;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.TermEvaluator;

/**
 * The count function. If no argument is given, then return the number of the input bindings. In other cases, the function expects
 * one argument, and return the number of input binding for which the value of the term is not null.
 * 
 * @author Yupeng
 * 
 */
public class CountFunction extends AbstractAggregateFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(CountFunction.class);
    
    public static final String  NAME = "COUNT";
    
    /**
     * The signatures of the function.
     * 
     * @author Michalis Petropoulos
     */
    private enum FunctionSignatureName
    {
        COUNT_INTEGER,

        COUNT_LONG,

        COUNT_FLOAT,

        COUNT_DOUBLE,

        COUNT_DECIMAL,

        COUNT_DATE,

        COUNT_TIMESTAMP,

        COUNT_BOOLEAN,

        COUNT_XHTML,

        COUNT_STRING,

        COUNT_STAR;
    }
    
    /**
     * The default constructor.
     */
    public CountFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        String arg_name = "value";
        
        signature = new FunctionSignature(FunctionSignatureName.COUNT_INTEGER.name(), TypeEnum.LONG.get());
        signature.addArgument(arg_name, TypeEnum.INTEGER.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.COUNT_LONG.name(), TypeEnum.LONG.get());
        signature.addArgument(arg_name, TypeEnum.LONG.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.COUNT_FLOAT.name(), TypeEnum.LONG.get());
        signature.addArgument(arg_name, TypeEnum.FLOAT.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.COUNT_DOUBLE.name(), TypeEnum.LONG.get());
        signature.addArgument(arg_name, TypeEnum.DOUBLE.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.COUNT_DECIMAL.name(), TypeEnum.LONG.get());
        signature.addArgument(arg_name, TypeEnum.DECIMAL.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.COUNT_DATE.name(), TypeEnum.LONG.get());
        signature.addArgument(arg_name, TypeEnum.DATE.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.COUNT_TIMESTAMP.name(), TypeEnum.LONG.get());
        signature.addArgument(arg_name, TypeEnum.TIMESTAMP.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.COUNT_BOOLEAN.name(), TypeEnum.LONG.get());
        signature.addArgument(arg_name, TypeEnum.BOOLEAN.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.COUNT_XHTML.name(), TypeEnum.LONG.get());
        signature.addArgument(arg_name, TypeEnum.XHTML.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.COUNT_STRING.name(), TypeEnum.LONG.get());
        signature.addArgument(arg_name, TypeEnum.STRING.get());
        this.addFunctionSignature(signature);
        
        signature = new FunctionSignature(FunctionSignatureName.COUNT_STAR.name(), TypeEnum.LONG.get());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public BindingValue evaluate(AggregateFunctionCall call, Collection<Binding> input, SetQuantifier set_quantifier)
            throws QueryExecutionException
    {
        Integer count = new Integer(0);
        
        if (call.getArguments().size() == 0)
        {
            count = input.size();
        }
        else
        {
            Term term = call.getArguments().get(0);
            Set<BindingValue> seen_value = new HashSet<BindingValue>();
            
            for (Binding in_binding : input)
            {
                BindingValue in_value = TermEvaluator.evaluate(term, in_binding);
                
                if (set_quantifier == SetQuantifier.DISTINCT)
                {
                    if (seen_value.contains(in_value))
                    // Have seen the value
                    continue;
                    else
                    // Record it as seen
                    seen_value.add(in_value);
                }
                
                if (!(in_value.getValue() instanceof NullValue))
                {
                    count++;
                }
            }
        }
        if (count == null)
        {
            return new BindingValue(new NullValue(LongType.class), true);
        }
        
        return new BindingValue(new LongValue(count), true);
    }
}
