/**
 * 
 */
package edu.ucsd.forward.fpl.plan;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.fpl.FplInterpretationException;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * Represents the instruction that returns to the function caller.
 * 
 * @author Yupeng
 * 
 */
public class ReturnInstruction extends AbstractInstruction
{
    @SuppressWarnings("unused")
    private static final Logger log          = Logger.getLogger(ReturnInstruction.class);
    
    private PhysicalPlan        m_return_plan;
    
    public static final String  RETURN_LABEL = "return";
    
    /**
     * Sets the return plan to execute.
     * 
     * @param return_plan
     *            the return plan to execute.
     */
    public void setQueryPlan(PhysicalPlan return_plan)
    {
        assert return_plan != null;
        m_return_plan = return_plan;
    }
    
    /**
     * Gets the return plan.
     * 
     * @return the return plan.
     */
    public PhysicalPlan getReturnPlan()
    {
        return m_return_plan;
    }
    
    @Override
    public String toExplainString()
    {
        String str = super.toExplainString() + "RETURN ";
        if (m_return_plan != null) str += m_return_plan.toExplainString();
        return str;
    }
    
    @Override
    public String execute(UnifiedApplicationState uas) throws FplInterpretationException
    {
        return "return";
    }
}
