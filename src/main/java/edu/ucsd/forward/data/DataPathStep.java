/**
 * 
 */
package edu.ucsd.forward.data;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.ucsd.app2you.util.collection.SmallMap;
import edu.ucsd.app2you.util.identity.DeepEquality;
import edu.ucsd.app2you.util.identity.EqualUtil;
import edu.ucsd.app2you.util.identity.HashCodeUtil;
import edu.ucsd.app2you.util.identity.Immutable;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.exception.UncheckedException;

/**
 * A data path step has a name and an optional predicate. The predicate can only be used for matching tuple values, and comprises
 * equality conditions for primary key attributes.
 * 
 * @author Kian Win Ong
 * @author Michalis Petropoulos
 * 
 */
public class DataPathStep implements Immutable, DeepEquality
{
    @SuppressWarnings("unused")
    private static final Logger log                     = Logger.getLogger(DataPathStep.class);
    
    /*
     * A data path step has string representation:
     * 
     * name [ key1 = value1, key2 = value2 ... ]
     * 
     * The name is compulsory, whereas the predicate (enclosed in square brackets) is optional.
     * 
     * Within the predicate, the key attribute names can contain only alphanumeric characters, underscore and period. The values are
     * URL-encoded, which guarantees that the only special characters that can occur are .-*_+%
     */

    public static final String  CHARSET                 = "UTF-8";
    
    public static final String  ATTRIBUTE_EQUAL         = "=";
    
    public static final String  PREDICATE_SEPARATOR     = ",";
    
    public static final String  PREDICATE_START         = "[";
    
    public static final String  PREDICATE_END           = "]";
    
    public static final Pattern ATTRIBUTE_NAME_PATTERN  = Pattern.compile("[a-zA-Z0-9_.]+");
    
    // Allows for empty strings
    public static final Pattern ATTRIBUTE_VALUE_PATTERN = Pattern.compile("[-a-zA-Z0-9.*_+%]*");
    
    private String              m_name;
    
    private Map<String, String> m_predicate;
    
    /**
     * Constructs a data path step with no predicate.
     * 
     * @param name
     *            the name.
     */
    public DataPathStep(String name)
    {
        assert (name != null);
        m_name = name;
    }
    
    /**
     * Constructs a data path step.
     * 
     * @param name
     *            the name.
     * @param predicate
     *            the predicate.
     */
    protected DataPathStep(String name, Map<String, String> predicate)
    {
        assert (name != null);
        assert (predicate != null);
        
        m_name = name;
        
        // Check if predicate is valid
        for (Map.Entry<String, String> entry : predicate.entrySet())
        {
            String attribute_name = entry.getKey();
            String attribute_value = entry.getValue();
            
            // assert (ATTRIBUTE_NAME_PATTERN.matcher(attribute_name).matches()) : ("Invalid attribute name " + attribute_name);
            // assert (ATTRIBUTE_VALUE_PATTERN.matcher(attribute_value).matches()) : ("Invalid attribute value " + attribute_value);
            // GWT compliant
            Matcher name_matcher = ATTRIBUTE_NAME_PATTERN.matcher(attribute_name);
            assert ((name_matcher.find() && name_matcher.start() == 0 && name_matcher.end() == attribute_name.length()));
            Matcher value_matcher = ATTRIBUTE_VALUE_PATTERN.matcher(attribute_value);
            assert ((value_matcher.find() && value_matcher.start() == 0 && value_matcher.end() == attribute_value.length()));
        }
        
        m_predicate = new SmallMap<String, String>(predicate);
    }
    
    /**
     * Returns the name.
     * 
     * @return the name.
     */
    public String getName()
    {
        return m_name;
    }
    
    /**
     * Returns the predicate.
     * 
     * @return the predicate if it exists; <code>null</code> otherwise.
     */
    public Map<String, String> getPredicate()
    {
        return (m_predicate != null) ? Collections.unmodifiableMap(m_predicate) : null;
    }
    
    @Override
    public Object[] getDeepEqualityObjects()
    {
        return new Object[] { m_name, m_predicate };
    }
    
    @Override
    public boolean equals(Object x)
    {
        return EqualUtil.equalsByDeepEquality(this, x);
    }
    
    @Override
    public int hashCode()
    {
        return HashCodeUtil.hashCodeByDeepEquality(this);
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(m_name);
        
        if (m_predicate != null)
        {
            sb.append(PREDICATE_START);
            boolean first = true;
            for (Map.Entry<String, String> entry : m_predicate.entrySet())
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    sb.append(PREDICATE_SEPARATOR);
                }
                
                sb.append(entry.getKey());
                sb.append(ATTRIBUTE_EQUAL);
                sb.append(entry.getValue());
            }
            sb.append(PREDICATE_END);
        }
        
        return sb.toString();
    }
    
    /**
     * Encodes a scalar value using URL encoding.
     * 
     * @param value
     *            the scalar value.
     * @return a URL-encoded string.
     */
    protected static String encode(ScalarValue value)
    {
        return encode(value.toString());
    }
    
    /**
     * Encodes a string using URL encoding.
     * 
     * @param string
     *            the string.
     * @return a URL-encoded string.
     */
    protected static String encode(String string)
    {
        try
        {
            return URLEncoder.encode(string, CHARSET);
        }
        catch (UnsupportedEncodingException e)
        {
            throw new UncheckedException(e);
        }
    }
    
    /**
     * Decodes a URL-encoded string.
     * 
     * @param url_encoded_string
     *            the URL-encoded string.
     * @return the decoded string.
     */
    protected static String decode(String url_encoded_string)
    {
        try
        {
            return URLDecoder.decode(url_encoded_string, CHARSET);
        }
        catch (UnsupportedEncodingException e)
        {
            throw new UncheckedException(e);
        }
    }
    
}
