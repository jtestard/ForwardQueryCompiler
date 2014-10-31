/**
 * 
 */
package edu.ucsd.forward.data.json;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.TypeException;
import edu.ucsd.forward.data.value.JsonValue;
import edu.ucsd.forward.exception.ExceptionMessages;

/**
 * A utility class for navigating in the JSON object given a query path.
 * 
 * @author Yupeng
 * 
 */
public final class JsonValueNavigationUtil
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(JsonValueNavigationUtil.class);
    
    /**
     * Hidden constructor.
     */
    private JsonValueNavigationUtil()
    {
        
    }
    
    /**
     * Navigates into the JSON value with the given string step.
     * 
     * @param value
     *            the JSON value to navigate into.
     * @param step
     *            the step in the navigation
     * @return the json object navigated into wrapped in the Java value.
     * @throws TypeException
     *             when the navigated object cannot be found
     */
    public static JsonValue navigate(JsonValue value, String step) throws TypeException
    {
        assert value != null && step != null;
        JsonElement element = value.getElement();
        JsonObject obj = element.getAsJsonObject();
        
        JsonElement member = obj.get(step);
        
        if (member == null)
        {
            throw new TypeException(ExceptionMessages.Type.ERROR_GETTING_JSON, step, obj.getClass().toString());
        }
        
        return new JsonValue(member);
    }
    
    /**
     * Gets the JSON values from a given JSON value as an array.
     * 
     * @param value
     *            the JSON value to iterate
     * @return the collection of the elements in JSON value.
     * @throws TypeException
     *             when the given JSON value is not array.
     */
    public static List<JsonValue> navigateCollection(JsonValue value) throws TypeException
    {
        assert value != null;
        List<JsonValue> list = new ArrayList<JsonValue>();
        
        JsonElement element = value.getElement();
        if (!element.isJsonArray()) throw new TypeException(ExceptionMessages.Type.ERROR_ITERATING_JSON, element.toString());
        JsonArray array = element.getAsJsonArray();
        
        for (JsonElement item : array)
        {
            list.add(new JsonValue(item));
        }
        return list;
    }
}
