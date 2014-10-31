/**
 * 
 */
package edu.ucsd.forward.fpl.plan;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * The abstract implementation of the control flow instruction.
 * 
 * @author Yupeng
 * 
 */
public abstract class AbstractControlFlowInstruction extends AbstractInstruction implements ControlFlowInstruction
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractControlFlowInstruction.class);
    
    private String              m_target_label;
    
    @Override
    public String getTargetLabel()
    {
        return m_target_label;
    }
    
    @Override
    public void setTargetLabel(String target_label)
    {
        assert target_label != null;
        m_target_label = target_label;
    }
}
