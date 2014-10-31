/**
 * 
 */
package edu.ucsd.forward.fpl.plan;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.fpl.FplInterpretationException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * Represents the instruction that jumps to the target instruction if the condition is satisfied, otherwise the following
 * instruction in sequence is executed next, as usual.
 * 
 * @author Yupeng
 * 
 */
public class ConditionalJumpInstruction extends AbstractControlFlowInstruction
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ConditionalJumpInstruction.class);
    
    private PhysicalPlan        m_condition_plan;
    
    private boolean             m_expected_value;
    
    /**
     * Constructs the conditional jump instruction with condition plan and the expected value.
     * 
     * @param expected_value
     *            the expected value of the condition when the instruction jumps to the target.
     * @param condition_plan
     *            the condition plan
     */
    public ConditionalJumpInstruction(boolean expected_value, PhysicalPlan condition_plan)
    {
        setConditionPlan(condition_plan);
        setExpectedValue(expected_value);
    }
    
    /**
     * Sets the expected value of the condition when the instruction jumps to the target.
     * 
     * @param expected_value
     *            the expected value of the condition when the instruction jumps to the target.
     */
    public void setExpectedValue(boolean expected_value)
    {
        m_expected_value = expected_value;
    }
    
    /**
     * Gets the expected value of the condition when the instruction jumps to the target.
     * 
     * @return the expected value of the condition when the instruction jumps to the target.
     */
    public boolean getExpectedValue()
    {
        return m_expected_value;
    }
    
    /**
     * Sets the condition plan.
     * 
     * @param condition_plan
     *            the condition plan, if result is <code>true</code>, then jumps to the target instruction.
     */
    public void setConditionPlan(PhysicalPlan condition_plan)
    {
        assert condition_plan != null;
        m_condition_plan = condition_plan;
    }
    
    /**
     * Gets the condition plan.
     * 
     * @return the condition plan.
     */
    public PhysicalPlan getConditionPlan()
    {
        return m_condition_plan;
    }
    
    @Override
    public String toExplainString()
    {
        String str = super.toExplainString();
        str += "IF " + m_condition_plan.toExplainString() + " IS " + m_expected_value + " GOTO " + getTargetLabel() + "\n";
        return str;
    }
    
    @Override
    public String execute(UnifiedApplicationState uas) throws FplInterpretationException
    {
        // Executes the query plan
        Value result = QueryProcessorFactory.getInstance().createEagerQueryResult(m_condition_plan, uas).getValue();
        
        Boolean actual_value;
        // Handle NullValue
        if (result instanceof NullValue)
        {
            actual_value = new Boolean(false);
        }
        else
        {
            actual_value = ((BooleanValue) result).getObject();
        }
        
        if (actual_value.equals(m_expected_value)) return getTargetLabel();
        
        return null;
    }
}
