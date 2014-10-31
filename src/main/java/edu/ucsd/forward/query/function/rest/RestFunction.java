/**
 * 
 */
package edu.ucsd.forward.query.function.rest;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.JsonType;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.SwitchType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.JsonValue;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.SwitchValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.AbstractFunction;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunction;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;

/**
 * A generic function that accesses REST services and takes as input the URL of the service, the HTTP verb and a tuple that contains
 * an arbitrary number of key/value pairs of parameters, and outputs a JSON data type.
 * 
 * @author Yupeng
 * 
 */
public class RestFunction extends AbstractFunction implements GeneralFunction
{
    @SuppressWarnings("unused")
    private static final Logger log           = Logger.getLogger(RestFunction.class);
    
    public static final String  NAME          = "REST";
    
    private static final String SUCCESS       = "success";
    
    private static final String ERROR         = "error";
    
    private static final String ERROR_MESSAGE = "error_message";
    
    private static final String HOST          = "host";
    
    private static final String PATH          = "path";
    
    private static final String VERB          = "verb";
    
    private static final String PARAMETERS    = "parameters";
    
    private static final String URL_ENCODING  = "UTF-8";
    
    private static final String SCHEME        = "scheme";
    
    private static final String PORT          = "port";
    
    /**
     * The signatures of the function.
     * 
     */
    private enum FunctionSignatureName
    {
        REST;
    }
    
    /**
     * Default constructor.
     */
    public RestFunction()
    {
        super(NAME);
        
        FunctionSignature signature;
        
        SwitchType return_type = new SwitchType();
        return_type.setCase(SUCCESS, new JsonType());
        TupleType error_type = new TupleType();
        error_type.setAttribute(ERROR_MESSAGE, new StringType());
        return_type.setCase(ERROR, error_type);
        signature = new FunctionSignature(FunctionSignatureName.REST.name(), return_type);
        signature.addArgument(HOST, new StringType());
        signature.addArgument(PATH, new StringType());
        signature.addArgument(SCHEME, new StringType());
        signature.addArgument(PORT, new IntegerType());
        signature.addArgument(VERB, new StringType());
        signature.addArgument(PARAMETERS, new TupleType());
        this.addFunctionSignature(signature);
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return false;
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        List<Value> arguments = evaluateArgumentsAndMatchFunctionSignature(call, input);
        
        String host = ((StringValue) arguments.get(0)).getObject();
        String path = ((StringValue) arguments.get(1)).getObject();
        String scheme = ((StringValue) arguments.get(2)).getObject();
        int port = ((IntegerValue) arguments.get(3)).getObject();
        String verb = ((StringValue) arguments.get(4)).getObject();
        TupleValue parameters = (TupleValue) arguments.get(5);
        
        // Get parameters.
        Map<String, String> query = new HashMap<String, String>();
        for (String attr_name : parameters.getAttributeNames())
        {
            Value para_value = parameters.getAttribute(attr_name);
            if (!(para_value instanceof ScalarValue))
            {
                return returnError("Only scalar value is supported as parameter value.");
            }
            String para_str = ((ScalarValue) para_value).getObject().toString();
            query.put(attr_name, para_str);
        }
        
        URI rest_uri;
        try
        {
            rest_uri = httpsUriFor(host, path, scheme, port, query);
        }
        catch (URISyntaxException e1)
        {
            return returnError("URI creation failed, " + e1.getMessage());
        }
        
        HttpClient client = new DefaultHttpClient();
        BindingValue return_binding = null;
        String response = null;
        try
        {
            HttpUriRequest request=null;
            if(verb.equalsIgnoreCase("get")) request = new HttpGet(rest_uri);
            else if(verb.equalsIgnoreCase("post")) request = new HttpPost(rest_uri);
            else return returnError("Unsupported verb "+verb);

            ResponseHandler<String> response_handler = new BasicResponseHandler();
            response = client.execute(request, response_handler);
            JsonParser parser = new JsonParser();
            JsonElement elem = parser.parse(response);
            SwitchValue return_value = new SwitchValue(SUCCESS, new JsonValue(elem));
            return_binding = new BindingValue(return_value, false);
        }
        catch (IOException e)
        {
            return_binding = returnError("URL execution fails, " + e.getMessage());
        }
        finally
        {
            client.getConnectionManager().shutdown();
        }
        return return_binding;
        
    }
    
    /**
     * Returns error output whenever the function fails.
     * 
     * @param msg
     *            the error message.
     * @return error message encoded in binding.
     */
    private BindingValue returnError(String msg)
    {
        TupleValue case_value = new TupleValue();
        case_value.setAttribute(ERROR_MESSAGE, new StringValue(msg));
        SwitchValue return_value = new SwitchValue(ERROR, case_value);
        return new BindingValue(return_value, false);
    }
    
    /**
     * Constructs an HTTPS URI. Throws RuntimeException when given invalid arguments.
     * 
     * @param host
     *            host part of the URI
     * @param path
     *            the path part of the URI
     * @param scheme
     *            the HTTP scheme
     * @param port
     *            port of the URI
     * @param query
     *            mapping of parameters to values for URI's "query" part
     * @return an HTTPS URI
     * @throws URISyntaxException
     *             when the URI creation fails.
     */
    private static URI httpsUriFor(String host, String path, String scheme, int port, Map<String, String> query)
            throws URISyntaxException
    {
        List<BasicNameValuePair> query_parameters = new ArrayList<BasicNameValuePair>();
        for (Entry<String, String> pair : query.entrySet())
        {
            query_parameters.add(new BasicNameValuePair(pair.getKey(), pair.getValue()));
        }
        
        String query_string = URLEncodedUtils.format(query_parameters, URL_ENCODING);
        final String fragment = null;
        
        return URIUtils.createURI(scheme, host, port, path, query_string, fragment);
        
    }
}
