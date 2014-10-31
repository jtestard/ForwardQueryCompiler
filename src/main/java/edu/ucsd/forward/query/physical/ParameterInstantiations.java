/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.LinkedHashMap;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.logical.term.Parameter;

/**
 * Represents parameter instantiations used by the query processor during physical plan execution.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class ParameterInstantiations
{
    @SuppressWarnings("unused")
    private static final Logger       log = Logger.getLogger(ParameterInstantiations.class);
    
    private Map<String, BindingValue> m_instantiations;
    
    /**
     * Constructor.
     */
    public ParameterInstantiations()
    {
        m_instantiations = new LinkedHashMap<String, BindingValue>();
    }
    
    /**
     * Checks whether the input parameter is instantiated or not.
     * 
     * @param param
     *            the parameter to check.
     * 
     * @return <code>true</code> if the parameter is instantiated; <code>false</code> otherwise.
     */
    public boolean isInstatiated(Parameter param)
    {
        return m_instantiations.containsKey(param.getId());
    }
    
    /**
     * Gets the binding value for the input parameter.
     * 
     * @param param
     *            the parameter to instantiate.
     * @return the instantiation of the input parameter.
     */
    public BindingValue getInstantiation(Parameter param)
    {
        BindingValue out = m_instantiations.get(param.getId());
        assert (out != null);
        
        return out;
    }
    
    /**
     * Sets the binding value for the input parameter. If the parameter is already instantiated, then the old value will be
     * overwritten.
     * 
     * @param param
     *            the parameter to instantiate.
     * @param value
     *            the binding value to use.
     */
    public void setInstantiation(Parameter param, BindingValue value)
    {
        m_instantiations.put(param.getId(), value);
    }
    
    /**
     * Resets the binding values for all parameters.
     */
    public void resetInstantiations()
    {
        m_instantiations.clear();
    }
    
}
