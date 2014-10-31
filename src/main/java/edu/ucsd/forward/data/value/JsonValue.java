/**
 * 
 */
package edu.ucsd.forward.data.value;

import java.util.Collection;
import java.util.Collections;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.JsonType;

/**
 * The JSON value that contains the JSON object.
 * 
 * @author Yupeng
 * 
 */
@SuppressWarnings("serial")
public class JsonValue extends AbstractValue<JsonType, AbstractComplexValue<?>>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(JsonValue.class);
    
    private JsonElement         m_element;
    
    /**
     * Constructs the JSON value.
     * 
     * @param element
     *            the JSON element.
     */
    public JsonValue(JsonElement element)
    {
        assert element != null;
        m_element = element;
    }
    
    /**
     * Gets the JSON element.
     * 
     * @return the JSON element.
     */
    public JsonElement getElement()
    {
        return m_element;
    }
    
    @Override
    public Class<? extends JsonType> getTypeClass()
    {
        return JsonType.class;
    }
    
    @Override
    public Collection<? extends Value> getChildren()
    {
        return Collections.<Value> emptyList();
    }
    
    /**
     * Parses the text into a JSON value.
     * 
     * @param text
     *            the string representation of JSON object.
     * @return the created JSON value.
     */
    public static JsonValue parse(String text)
    {
        JsonElement element = new JsonParser().parse(text);
        return new JsonValue(element);
    }
    
    /**
     * Gets the value in string format.
     * 
     * @return the value in string format.
     */
    public String getString()
    {
        return m_element.getAsString();
    }
    
    @Override
    public String toString()
    {
        return m_element.toString();
    }
}
