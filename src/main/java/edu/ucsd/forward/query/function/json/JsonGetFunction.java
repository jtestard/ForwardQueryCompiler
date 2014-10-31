/**
 * 
 */
package edu.ucsd.forward.query.function.json;

import java.io.IOException;

import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.type.JsonType;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.value.JsonValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.AbstractFunction;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunction;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * A general-purpose JSON function call that issues an http-get request and returns the result as a JSON object.
 * 
 * @author Yupeng Fu
 * 
 */
public class JsonGetFunction extends AbstractFunction implements GeneralFunction
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(JsonGetFunction.class);
    
    public static final String  NAME = "JSON_GET";
    
    /**
     * Default constructor.
     */
    public JsonGetFunction()
    {
        super(NAME);
        m_data_source = DataSource.MEDIATOR;
        FunctionSignature signature = new FunctionSignature(NAME, new JsonType());
        signature.addArgument("url", new StringType());
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
        StringValue argu = (StringValue) evaluateArgumentsAndMatchFunctionSignature(call, input).get(0);
        String url = argu.getObject();
        
        HttpClient client = new DefaultHttpClient();
        String response = null;
        try
        {
            HttpGet get_req = new HttpGet(url);
            ResponseHandler<String> response_handler = new BasicResponseHandler();
            response = client.execute(get_req, response_handler);
        }
        catch (IOException e)
        {
            throw new RuntimeException("JSON GET call fails, because " + e.getMessage());
        }
        finally
        {
            client.getConnectionManager().shutdown();
        }
        
        JsonParser parser = new JsonParser();
        JsonElement elem = parser.parse(response);
        JsonValue json_value = new JsonValue(elem);
        return new BindingValue(json_value, false);
    }
}
