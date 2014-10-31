/**
 * 
 */
package edu.ucsd.forward.fpl.plan;

/**
 * Represents the instruction that controls the execution flow.
 * 
 * @author Yupeng
 * 
 */
public interface ControlFlowInstruction extends Instruction
{
    /**
     * Gets the label of the target instruction.
     * 
     * @return the label of the target instruction.
     */
    String getTargetLabel();
    
    /**
     * Sets the label of the target instruction.
     * 
     * @param target_label
     *            the label of the target instruction.
     */
    void setTargetLabel(String target_label);
    
}
