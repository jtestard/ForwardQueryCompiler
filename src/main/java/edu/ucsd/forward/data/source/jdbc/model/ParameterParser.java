/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.model;

import java.util.Collections;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A parser for SQL statement strings that will parse a SQL string with named parameters, thereafter provide a vanilla SQL string
 * with question mark '?' positional parameters and a map of positional parameters to named parameters. A named parameter must start
 * with colon ':', and contains letters, digits, periods '.' and underscores '_'.
 * 
 * @author Kian Win
 * 
 */
public class ParameterParser
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ParameterParser.class);
    
    private boolean             m_used;
    
    private ParameterMap        m_parameter_map;
    
    private String              m_named_parameters_string;
    
    private String              m_positional_parameters_string;
    
    /**
     * Constructs the parser.
     * 
     * @param named_parameters_string
     *            - the SQL string with named parameters.
     */
    public ParameterParser(String named_parameters_string)
    {
        assert (named_parameters_string != null);
        m_named_parameters_string = named_parameters_string;
    }
    
    /**
     * Returns the SQL string with named parameters.
     * 
     * @return the SQL string with named parameters.
     */
    public String getNamedParametersString()
    {
        return m_named_parameters_string;
    }
    
    /**
     * Returns whether this parser has already been used.
     * 
     * @return <code>true</code> if the parser has already been used; <code>false</code> otherwise.
     */
    public boolean isUsed()
    {
        return m_used;
    }
    
    /**
     * Runs the parser.
     * 
     */
    public void parse()
    {
        parse(Collections.<String, String> emptyMap());
    }
    
    /**
     * Runs the parser. A replace map can be provided to indicate the replacement for each parameter found. If the replace map is
     * empty, the <code>?</code> character will be used for all replacements.
     * 
     * @param replace_map
     *            - the replace map.
     */
    public void parse(Map<String, String> replace_map)
    {
        assert (!isUsed());
        m_used = true;
        
        String s = m_named_parameters_string;
        
        /*-
         * Adapted from Adam Crume's SQL parsing code found at: 
         * http://www.javaworld.com/javaworld/jw-04-2007/jw-04-jdbc.html
         */

        // Instead of using regular expressions, track whether we are inside quotes to avoid parsing within strings that look like
        // parameters.
        int length = s.length();
        int position = 1;
        boolean in_single_quote = false;
        boolean in_double_quote = false;
        StringBuilder sb = new StringBuilder(length);
        ParameterMap parameter_map = new ParameterMap();
        
        for (int i = 0; i < length; i++)
        {
            char c = s.charAt(i);
            Parameter parameter = null;
            
            if (in_single_quote)
            {
                // Within a single quote, and the next character is a single quote
                if (c == '\'')
                {
//                    if (i + 1 < length && s.charAt(i + 1) == '\'')
//                    {
//                        // SQL stupidly uses two single quote characters to denote a single quote within a string
//                        i += 1;
//                    }
//                    else
                    {
                        // Exiting single quote
                        in_single_quote = false;
                    }
                }
            }
            else if (in_double_quote)
            {
                // Exiting double quote
                if (c == '"')
                {
                    in_double_quote = false;
                }
            }
            else
            {
                if (c == '\'')
                {
                    // Entering single quote
                    in_single_quote = true;
                }
                else if (c == '"')
                {
                    // Entering double quote
                    in_double_quote = true;
                }
                else if (isParameterStart(c) && i + 1 < length && isParameterPart(s.charAt(i + 1)))
                {
                    // Finally, a parameter is found
                    int j = i + 2;
                    while (j < length && isParameterPart(s.charAt(j)))
                    {
                        j++;
                    }
                    
                    // Copy the parameter name
                    parameter = new Parameter(s.substring(i + 1, j));
                    
                    // Skip past the end of the parameter
                    i += parameter.getName().length();
                    
                    // Remember which position the parameter occurred in
                    parameter_map.addParameterPosition(parameter, position++);
                }
            }
            
            if (parameter == null)
            {
                sb.append(c);
            }
            else if (replace_map.isEmpty())
            {
                sb.append("?");
            }
            else if (replace_map.containsKey(parameter.getName()))
            {
                sb.append(replace_map.get(parameter.getName()));
            }
            else
            {
                assert (false) : "Parameter \"" + parameter + "\" is not bound";
            }
        }
        
        m_parameter_map = parameter_map;
        m_positional_parameters_string = sb.toString();
    }
    
    /**
     * Returns whether a character can start a parameter.
     * 
     * @param c
     *            - the character.
     * @return <code>true</code> if the character can start a parameter; <code>false</code> otherwise.
     */
    public static boolean isParameterStart(char c)
    {
        // Note that the dollar sign cannot be used, as Postgresql uses it for dollar quoting.
        return c == ':';
    }
    
    /**
     * Returns whether a character can be part of a parameter.
     * 
     * @param c
     *            - the character.
     * @return <code>true</code> if the character can be part of a parameter; <code>false</code> otherwise.
     */
    public static boolean isParameterPart(char c)
    {
        return Character.isLetterOrDigit(c) || c == '/' || c == '.' || c == '_' || c == '-';
    }
    
    /**
     * Returns the parameter map.
     * 
     * @return the parameter map.
     */
    public ParameterMap getParameterMap()
    {
        assert (isUsed());
        return m_parameter_map;
    }
    
    /**
     * Returns the SQL string with ? positional parameters.
     * 
     * @return the SQL string with ? positional parameters.
     */
    public String getPositionalParametersString()
    {
        assert (isUsed());
        return m_positional_parameters_string;
    }
}
