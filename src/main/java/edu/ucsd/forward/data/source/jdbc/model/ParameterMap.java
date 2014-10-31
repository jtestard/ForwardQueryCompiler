/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * Maps a parameter name to its relative positions within a SQL command. The positions are 1-indexed.
 * 
 * @author Kian Win
 * 
 */
public class ParameterMap
{
    @SuppressWarnings("unused")
    private static final Logger           log = Logger.getLogger(ParameterMap.class);
    
    private Map<Parameter, List<Integer>> m_map;
    
    /**
     * Constructs the map.
     * 
     */
    public ParameterMap()
    {
        m_map = new LinkedHashMap<Parameter, List<Integer>>();
    }
    
    /**
     * Adds an occurence of a parameter at a relative position.
     * 
     * @param parameter
     *            - the parameter.
     * @param position
     *            - the relative position.
     */
    public void addParameterPosition(Parameter parameter, int position)
    {
        assert (parameter != null);
        assert (position >= 0);
        
        List<Integer> list = m_map.get(parameter);
        if (list == null)
        {
            list = new ArrayList<Integer>();
            m_map.put(parameter, list);
        }
        
        list.add(position);
    }
    
    /**
     * Returns the parameters.
     * 
     * @return the parameters.
     * 
     */
    public Collection<Parameter> getParameters()
    {
        return Collections.unmodifiableCollection(m_map.keySet());
    }
    
    /**
     * Returns the relative positions for a parameter.
     * 
     * @param parameter
     *            - the parameter.
     * @return the relative positions for a parameter.
     */
    public List<Integer> getPositions(Parameter parameter)
    {
        List<Integer> list = m_map.get(parameter);
        if (list == null) list = Collections.emptyList();
        return Collections.unmodifiableList(list);
    }
}
