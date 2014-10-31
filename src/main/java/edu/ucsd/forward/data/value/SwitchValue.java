/**
 * 
 */
package edu.ucsd.forward.data.value;

import java.util.Arrays;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.SwitchType;

/**
 * A switch value.
 * 
 * @author Kian Win
 * @author Yupeng
 */
@SuppressWarnings("serial")
public class SwitchValue extends AbstractComplexValue<SwitchType>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(SwitchValue.class);
    
    private String              m_case_name;
    
    private Value               m_case_value;
    
    @Override
    public Class<SwitchType> getTypeClass()
    {
        return SwitchType.class;
    }
    
    /**
     * Default constructor.
     */
    public SwitchValue()
    {
    }
    
    /**
     * Constructs the switch value.
     * 
     * @param case_name
     *            - the name of the selected case.
     * @param case_value
     *            - the value of the selected case.
     */
    public SwitchValue(String case_name, Value case_value)
    {
        setCase(case_name, case_value);
    }
    
    /**
     * Sets the selected case.
     * 
     * @param case_name
     *            The case name.
     * @param case_value
     *            The case value.
     */
    @SuppressWarnings("unchecked")
    public void setCase(String case_name, Value case_value)
    {
        assert (case_name != null) : "Case name must be non-null";
        
        m_case_name = case_name;
        
        if (m_case_value != null && m_case_value.getParent() != null)
        {
            ((AbstractValue<SwitchType, Value>) m_case_value).setParent(null);
        }
        m_case_value = case_value;
        if (m_case_value != null)
        {
            ((AbstractValue<?, SwitchValue>) m_case_value).setParent(this);
        }
    }
    
    @Override
    public List<Value> getChildren()
    {
        return Arrays.asList(m_case_value);
    }
    
    /**
     * Returns the name of the selected case.
     * 
     * @return the name of the selected case.
     */
    public String getCaseName()
    {
        return m_case_name;
    }
    
    /**
     * Returns the value of the selected case.
     * 
     * @return the value of the selected case.
     */
    public Value getCase()
    {
        return m_case_value;
    }
    
    @Override
    public String toString()
    {
        return "<" + m_case_name + " : " + m_case_value + ">";
    }
    
}
