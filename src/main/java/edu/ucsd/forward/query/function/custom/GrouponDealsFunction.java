/**
 * 
 */
package edu.ucsd.forward.query.function.custom;

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
import org.apache.http.client.utils.URIUtils;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import edu.ucsd.forward.data.json.JsonToValueConverter;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.DoubleType;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.LongType;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.DoubleValue;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.function.string.AbstractStringFunction;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.TermEvaluator;

/**
 * A function for querying Groupon for a list of deals.
 * 
 * @author Christopher Rebert
 */
public class GrouponDealsFunction extends AbstractStringFunction// FIXME: right superclass?
{
    public static final String  NAME             = "groupon_deals";
    
    private static final String CLIENT_ID_VAL    = "41bf74c1fdf557dfbab1a13898cb8ad741a406cd";
    
    private static final String HTTPS_SCHEME     = "https";
    
    private static final int    HTTPS_PORT       = 443;
    
    private static final String GROUPON_API_HOST = "api.groupon.com";
    
    private static final String DEALS_JSON_PATH  = "v2/deals.json";
    
    private static final String URL_ENCODING     = "UTF-8";
    
    private static final String DEALS_ATTR       = "deals";
    
    // API parameters
    private static final String CLIENT_ID_PARAM  = "client_id";
    
    private static final String POSTAL_CODE      = "postal_code";
    
    private static final String LATITUDE         = "lat";
    
    private static final String LONGITUDE        = "lng";
    
    private static final String RADIUS           = "radius";
    
    private static final String AREA             = "area";
    
    private static final String DIVISION_ID      = "division_id";
    
    private static final String EMAIL            = "email_address";
    
    private static final String DEVICE_TOKEN     = "device_token";
    
    private static final String SUBSCRIBER       = "subscriber_id";
    
    private static final String AFFILIATE        = "affiliate";
    
    private static final String CHANNEL          = "channel_id";
    
    private static final String PROMOTION        = "promotions";
    
    private Type                m_return_type;
    
    /**
     * The default constructor.
     */
    public GrouponDealsFunction()
    {
        super(NAME);
        m_return_type = getReturnType();
        this.addFunctionSignature(getSignature(m_return_type));
    }
    
    /**
     * Constructs the FORWARD Type describing the return type of the groupon_deals function.
     * 
     * @return the return type of the groupon_deals function
     */
    private static Type getReturnType()
    {
        CollectionType deals = new CollectionType();// root type
        TupleType deal = deals.getTupleType();
        // simple deal attributes
        deal.setAttribute("id", new StringType());
        deal.setAttribute("type", new StringType());
        deal.setAttribute("title", new StringType());
        deal.setAttribute("announcement_title", new StringType());
        // deal.setAttribute("highlights_html", new StringType());
        // deal.setAttribute("pitch_html", new StringType());
        deal.setAttribute("small_image_url", new StringType());
        deal.setAttribute("medium_image_url", new StringType());
        deal.setAttribute("large_image_url", new StringType());
        deal.setAttribute("sidebar_image_url", new StringType());
        deal.setAttribute("deal_url", new StringType());
        
        // deal's types
        CollectionType deal_types = new CollectionType();
        deal.setAttribute("deal_types", deal_types);
        TupleType deal_type = deal_types.getTupleType();
        deal_type.setAttribute("id", new StringType());
        deal_type.setAttribute("name", new StringType());
        
        // deal's merchant
        TupleType merchant = new TupleType();
        deal.setAttribute("merchant", merchant);
        merchant.setAttribute("id", new StringType());
        merchant.setAttribute("name", new StringType());
        merchant.setAttribute("website_url", new StringType());
        
        // merchant's ratings
        CollectionType ratings = new CollectionType();
        merchant.setAttribute("ratings", ratings);
        TupleType rating = ratings.getTupleType();
        rating.setAttribute("id", new LongType());
        rating.setAttribute("rating", new DoubleType());
        rating.setAttribute("reviews_count", new IntegerType());
        rating.setAttribute("url", new StringType());
        rating.setAttribute("link_text", new StringType());
        
        // deal's channels
        CollectionType deal_channels = new CollectionType();
        deal.setAttribute("channels", deal_channels);
        TupleType deal_channel = deal_channels.getTupleType();
        deal_channel.setAttribute("id", new StringType());
        deal_channel.setAttribute("name", new StringType());
        
        // deal's tags
        CollectionType deal_tags = new CollectionType();
        deal.setAttribute("tags", deal_tags);
        TupleType deal_tag = deal_tags.getTupleType();
        deal_tag.setAttribute("name", new StringType());
        
        // deal's options
        CollectionType options = new CollectionType();
        deal.setAttribute("options", options);
        TupleType option = options.getTupleType();
        option.setAttribute("id", new LongType());
        
        // option's price
        TupleType price = new TupleType();
        option.setAttribute("price", price);
        price.setAttribute("amount", new IntegerType());
        price.setAttribute("formattedAmount", new StringType());
        
        // option's redemption locations
        CollectionType redemption_locations = new CollectionType();
        option.setAttribute("redemption_locations", redemption_locations);
        TupleType redemption_location = redemption_locations.getTupleType();
        redemption_location.setAttribute("id", new LongType());
        redemption_location.setAttribute("lng", new DoubleType());
        redemption_location.setAttribute("lat", new DoubleType());
        // redemption_location.setAttribute("name", new StringType());
        redemption_location.setAttribute("neighborhood", new StringType());
        redemption_location.setAttribute("streetAddress1", new StringType());
        // redemption_location.setAttribute("streetAddress2", new StringType());
        // redemption_location.setAttribute("city", new StringType());
        // redemption_location.setAttribute("state", new StringType());
        // redemption_location.setAttribute("postalCode", new StringType());
        
        return deals;
    }
    
    /**
     * Returns the function signature for the groupon_deals function.
     * 
     * @param return_type
     *            the return type of the groupon_deals function
     * @return the function signature
     */
    private static FunctionSignature getSignature(Type return_type)
    {
        final Type string_type = TypeEnum.STRING.get();
        // final Type double_type = TypeEnum.DOUBLE.get();
        
        // all arguments are optional and filter the results
        
        FunctionSignature postcode_sig = new FunctionSignature(NAME, return_type);
        postcode_sig.addArgument(POSTAL_CODE, string_type);/* could be validated further */
        
        // FunctionSignature lat_long_sig = new FunctionSignature(NAME, return_type);
        // lat_long_sig.addArgument(LATITUDE, double_type);
        // lat_long_sig.addArgument(LONGITUDE, double_type);
        
        // FunctionSignature lat_long_radius_sig = new FunctionSignature(NAME, return_type);
        // lat_long_radius_sig.addArgument(LATITUDE, double_type);
        // lat_long_radius_sig.addArgument(LONGITUDE, double_type);
        // // in miles, requires lat & lng to be spec'd; possibly allows double
        // lat_long_radius_sig.addArgument(RADIUS, TypeEnum.INTEGER.get());
        
        // sig.addArgument(AREA, string_type);
        // sig.addArgument(DIVISION_ID, string_type);
        
        // // other filters
        // final String[] string_params = { EMAIL, DEVICE_TOKEN, SUBSCRIBER, AFFILIATE, CHANNEL, PROMOTION, };
        // for (String param_name : string_params)
        // {
        // sig.addArgument(param_name, string_type);
        // }
        
        return postcode_sig;
    }
    
    /**
     * Evaluates a function's arguments under some variable bindings.
     * 
     * @param arguments
     *            the list of unevaluated argument terms to evaluate
     * @param bindings
     *            the bindings under which to evaluate the argument terms
     * @return a list of argument values
     * @throws QueryExecutionException
     *             thrown when there is an error evaluating an argument
     */
    private static List<BindingValue> evaluateArguments(List<Term> arguments, Binding bindings) throws QueryExecutionException
    {
        List<BindingValue> values = new ArrayList<BindingValue>();
        for (Term term : arguments)
        {
            values.add(TermEvaluator.evaluate(term, bindings));
        }
        return values;
    }
    
    /**
     * If the value is null, does nothing. Otherwise, extracts the StringValue and puts it into the map as a Java String under the
     * given key.
     * 
     * @param map
     *            the map to possibly put the entry into
     * @param key
     *            key to put the value under
     * @param binding_val
     *            value to check for nullness and possibly put into the map as a value
     */
    private static void putStringIfNotNull(Map<String, String> map, String key, BindingValue binding_val)
    {
        Value val = binding_val.getValue();
        if (val instanceof NullValue)
        {
            return;
        }
        StringValue str_val = (StringValue) val;
        map.put(key, str_val.getObject());
    }
    
    /**
     * If the value is null, does nothing. Otherwise, extracts the DoubleValue and puts it into the map as a Java String under the
     * given key.
     * 
     * @param map
     *            the map to possibly put the entry into
     * @param key
     *            key to put the value under
     * @param binding_val
     *            value to check for nullness and possibly put into the map as a value
     */
    private static void putDoubleIfNotNull(Map<String, String> map, String key, BindingValue binding_val)
    {
        Value val = binding_val.getValue();
        if (val instanceof NullValue)
        {
            return;
        }
        DoubleValue dbl_val = (DoubleValue) val;
        map.put(key, "" + (dbl_val.getObject()));
    }
    
    /**
     * If the value is null, does nothing. Otherwise, extracts the IntegerValue and puts it into the map as a Java String under the
     * given key.
     * 
     * @param map
     *            the map to possibly put the entry into
     * @param key
     *            key to put the value under
     * @param binding_val
     *            value to check for nullness and possibly put into the map as a value
     */
    private static void putIntegerIfNotNull(Map<String, String> map, String key, BindingValue binding_val)
    {
        Value val = binding_val.getValue();
        if (val instanceof NullValue)
        {
            return;
        }
        IntegerValue dbl_val = (IntegerValue) val;
        map.put(key, "" + (dbl_val.getObject()));
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        List<Term> args = call.getArguments();
        assert args.size() == 1;// 12;
        List<BindingValue> arg_vals = evaluateArguments(args, input);
        
        // unpack args and repack as map
        Map<String, String> query = new HashMap<String, String>();
        int i = 0;
        
        putStringIfNotNull(query, POSTAL_CODE, arg_vals.get(i++));
        // putDoubleIfNotNull(query, LATITUDE, arg_vals.get(i++));
        // putDoubleIfNotNull(query, LONGITUDE, arg_vals.get(i++));
        // putIntegerIfNotNull(query, RADIUS, arg_vals.get(i++));
        // putStringIfNotNull(query, AREA, arg_vals.get(i++));
        
        // BindingValue div_id = arg_vals.get(i++);
        // // Per Groupon docs, if both spec'd, division_id will dominate over lat+lng+radius
        // if (!(div_id.getValue() instanceof NullValue))
        // {
        // query.remove(LATITUDE);
        // query.remove(LONGITUDE);
        // query.remove(RADIUS);
        // }
        
        // putStringIfNotNull(query, EMAIL, arg_vals.get(i++));
        // putStringIfNotNull(query, DEVICE_TOKEN, arg_vals.get(i++));
        // putStringIfNotNull(query, SUBSCRIBER, arg_vals.get(i++));
        // putStringIfNotNull(query, AFFILIATE, arg_vals.get(i++));
        // putStringIfNotNull(query, CHANNEL, arg_vals.get(i++));
        // putStringIfNotNull(query, PROMOTION, arg_vals.get(i++));
        
        // call API
        Value deals = callDealsRestApi(query);
        if (deals == null)
        {
            // FIXME: handle more elegantly
            throw new RuntimeException("Error communicating with Groupon");
        }
        
        return new BindingValue(deals, true);
    }
    
    /**
     * Makes the given deals query against the Groupon REST API.
     * 
     * @param query
     *            query string arguments to the Groupon API call to list deals
     * @return a FORWARD collection of the deals, or null if there was an error communicating with Groupon
     */
    private Value callDealsRestApi(Map<String, String> query)
    {
        if (query.containsKey(RADIUS) && !(query.containsKey(LATITUDE) && query.containsKey(LONGITUDE)))
        {
            // FIXME: report this error better
            throw new RuntimeException("Called groupon_deals with radius argument without also specifying lat and lng");
        }
        if (query.containsKey(LATITUDE) != query.containsKey(LONGITUDE))
        {
            // FIXME: report this error better
            throw new RuntimeException("Called groupon_deals but specified only one of lat and lng; must specify both or neither.");
        }
        // FIXME: could validate email
        // FIXME: could validate postal code
        
        query.put(CLIENT_ID_PARAM, CLIENT_ID_VAL);// set client ID
        URI rest_uri = httpsUriFor(GROUPON_API_HOST, DEALS_JSON_PATH, query);
        
        HttpClient client = new DefaultHttpClient();
        String response = null;
        try
        {
            HttpGet get_req = new HttpGet(rest_uri);
            ResponseHandler<String> response_handler = new BasicResponseHandler();
            response = client.execute(get_req, response_handler);
        }
        catch (IOException e)
        {
            return null;
        }
        finally
        {
            client.getConnectionManager().shutdown();
        }
        
        return parseGrouponDeals(response);
    }
    
    /**
     * Parses the Groupon deals JSON into a FORWARD Value.
     * 
     * @param json
     *            the JSON response from the Groupon deals REST API
     * @return a FORWARD Value corresponding to the JSON results
     */
    private Value parseGrouponDeals(String json)
    {
        JsonParser parser = new JsonParser();
        JsonElement root_elem = parser.parse(json);
        JsonObject root_obj = root_elem.getAsJsonObject();
        JsonElement deals = root_obj.get(DEALS_ATTR);
        return JsonToValueConverter.convert(deals, m_return_type);
    }
    
    /**
     * Constructs an HTTPS URI. Throws RuntimeException when given invalid arguments.
     * 
     * @param host
     *            "host" part of the URI
     * @param path
     *            "path" part of the URI
     * @param query
     *            mapping of parameters to values for URI's "query" part
     * @return an HTTPS URI
     */
    private static URI httpsUriFor(String host, String path, Map<String, String> query)
    {
        List<BasicNameValuePair> query_parameters = new ArrayList<BasicNameValuePair>();
        for (Entry<String, String> pair : query.entrySet())
        {
            query_parameters.add(new BasicNameValuePair(pair.getKey(), pair.getValue()));
        }
        
        String query_string = URLEncodedUtils.format(query_parameters, URL_ENCODING);
        final String fragment = null;
        
        try
        {
            return URIUtils.createURI(HTTPS_SCHEME, host, HTTPS_PORT, path, query_string, fragment);
        }
        catch (URISyntaxException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return false;
    }
}
