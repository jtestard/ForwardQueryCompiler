/**
 * 
 */
package edu.ucsd.forward.query.function.json;

import com.google.gson.JsonArray;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.JsonType;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.JsonValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.AbstractFunction;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunction;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.TermEvaluator;

/**
 * A function that gets the JSON value associated with the index from the input JSON array. If the input is not array or the length
 * of the array is smaller than the index, throws runtime exception.
 * 
 * @author Yupeng Fu
 * 
 */
public class JsonArrayGetByIndex extends AbstractFunction implements GeneralFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(JsonArrayGetByIndex.class);
    
    public static final String  NAME = "JSON_ARRAY_GET_BY_INDEX";
    
    /**
     * Default constructor.
     */
    public JsonArrayGetByIndex()
    {
        super(NAME);
        m_data_source = DataSource.MEDIATOR;
        FunctionSignature signature = new FunctionSignature(NAME, new JsonType());
        signature.addArgument("array", new JsonType());
        signature.addArgument("index", new IntegerType());
        addFunctionSignature(signature);
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return false;
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        BindingValue arg1 = TermEvaluator.evaluate(call.getArguments().get(0), input);
        BindingValue arg2 = TermEvaluator.evaluate(call.getArguments().get(1), input);
        JsonValue json_in = (JsonValue) arg1.getValue();
        JsonArray array = json_in.getElement().getAsJsonArray();
        int index = ((IntegerValue) arg2.getValue()).getObject();
        JsonValue json_out = new JsonValue(array.get(index));
        return new BindingValue(json_out, false);
    }
}
