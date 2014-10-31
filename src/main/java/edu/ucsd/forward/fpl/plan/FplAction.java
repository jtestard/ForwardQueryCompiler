/**
 * 
 */
package edu.ucsd.forward.fpl.plan;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.fpl.ast.ActionDefinition;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * Represents an FPL action that consists of a physical plan calling an FPL function.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class FplAction
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(FplAction.class);
    
    private String              m_path;
    
    private String              m_function_name;
    
    private PhysicalPlan        m_plan;
    
    /**
     * Constructs an empty function plan given a definition. The function plan initially has no instructions.
     * 
     * @param definition
     *            the action definition.
     * @param plan
     *            the action plan.
     */
    public FplAction(ActionDefinition definition, PhysicalPlan plan)
    {
        this(definition.getPath(), definition.getFunctionName(), plan);
    }
    
    public FplAction(String action_path, String func_name, PhysicalPlan plan)
    {
        assert action_path != null;
        assert func_name != null;
        m_path = action_path;
        m_function_name = func_name;
        
        assert (plan != null);
        m_plan = plan;
    }
    
    /**
     * Gets the path to the action.
     * 
     * @return the path to the action.
     */
    public String getPath()
    {
        return m_path;
    }
    
    /**
     * Gets the name of the associated function.
     * 
     * @return the name of the associated function.
     */
    public String getFunctionName()
    {
        return m_function_name;
    }
    
    /**
     * Gets the action plan.
     * 
     * @return the action plan.
     */
    public PhysicalPlan getPlan()
    {
        return m_plan;
    }
    
}
