/**
 * 
 */
package edu.ucsd.forward.data.mapping;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.ParseException;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.DataPath;
import edu.ucsd.forward.data.DataPathStep;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.UncheckedException;

/**
 * The mapping that maps an attribute in the source data tree to the attribute in the target data tree. For now the mapping is only
 * used in page compiler that maps the context state to the page state.
 * 
 * @author Yupeng Fu
 * 
 */
public class Mapping
{
    @SuppressWarnings("unused")
    private static final Logger log             = Logger.getLogger(Mapping.class);
    
    private DataPath            m_src_path;
    
    private String              m_dst_path_str;
    
    private DataPath            m_dst_path;
    
    private List<DataPath>      m_parameters;
    
    public static final String  PARAMETER_TOKEN = "?";
    
    /**
     * Constructs the mapping.
     */
    public Mapping(DataPath src_path, String dst_path_str, List<DataPath> predicates)
    {
        assert src_path != null;
        assert dst_path_str != null;
        assert predicates != null;
        
        m_src_path = src_path;
        m_dst_path_str = dst_path_str;
        m_parameters = predicates;
        // FIXME: Restore the check that the number of parameters in the dst path string are the same as the predicates. The check
        // is now disabled because it's not supported by GWT.
        // int count = StringUtils.countMatches(m_dst_path_str, PARAMETER_TOKEN);
        // assert count == predicates.size();
    }
    
    /**
     * Gets the data path to the source attribute.
     * 
     * @return the data path to the source attribute.
     */
    public DataPath getSourcePath()
    {
        return m_src_path;
    }
    
    /**
     * Gets the parameters.
     * 
     * @return the parameters.
     */
    public List<DataPath> getParameters()
    {
        return m_parameters;
    }
    
    /**
     * Gets the data path to the target attribute.
     * 
     * @return the data path to the target attribute.
     */
    public DataPath getTargetPath()
    {
        return m_dst_path;
    }
    
    /**
     * Gets the path to the target attribute before instantiation in string.
     * 
     * @return the path to the target attribute before instantiation in string.
     */
    public String getTargetPathString()
    {
        return m_dst_path_str;
    }
    
    /**
     * Instantiates the data path to the target attribute by providing the event state.
     * 
     * @param event_state
     *            the event state corresponding to the action call.
     */
    public void instantiate(DataTree event_state)
    {
        String instantiated_path = m_dst_path_str;
        for (DataPath path : m_parameters)
        {
            Value parameter_value = path.find(event_state);
            assert parameter_value != null && parameter_value instanceof ScalarValue;
            String parameter_string = null;
            try
            {
                parameter_string = URLEncoder.encode(((ScalarValue) parameter_value).getObject().toString(), DataPathStep.CHARSET);
            }
            catch (UnsupportedEncodingException e)
            {
                throw new UncheckedException(e);
            }
            instantiated_path = instantiated_path.replaceFirst("\\" + PARAMETER_TOKEN, parameter_string);
        }
        
        try
        {
            m_dst_path = new DataPath(instantiated_path);
        }
        catch (ParseException e)
        {
            // should not happen
            throw new AssertionError();
        }
    }
    
    @Override
    public String toString()
    {
        String string = m_src_path.toString() + " <=> ";
        if (m_dst_path != null) string += m_dst_path.toString();
        else string += m_dst_path_str;
        return string;
    }
}
