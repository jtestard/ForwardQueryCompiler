/**
 * 
 */
package edu.ucsd.forward.fpl.plan;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.fpl.FplInterpretationException;

/**
 * Represents the unconditional jump 'goto' the target instruction. The instruction with the target label is the next to be
 * executed.
 * 
 * @author Yupeng
 * 
 */
public class UnconditionalJumpInstruction extends AbstractControlFlowInstruction
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(UnconditionalJumpInstruction.class);
    
    /**
     * Constructs the unconditional jump instruction with the target label.
     * 
     * @param target_label
     *            the unconditional jump instruction with the target label.
     */
    public UnconditionalJumpInstruction(String target_label)
    {
        setTargetLabel(target_label);
    }
    
    @Override
    public String toExplainString()
    {
        return super.toExplainString() + "GOTO " + getTargetLabel() + "\n";
    }
    
    @Override
    public String execute(UnifiedApplicationState uas) throws FplInterpretationException
    {
        return getTargetLabel();
    }
}
