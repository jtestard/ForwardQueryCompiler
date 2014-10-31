/**
 * 
 */
package edu.ucsd.forward.fpl.plan;

import java.util.Set;

import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.fpl.FplInterpretationException;
import edu.ucsd.forward.query.explain.ExplanationPrinter;

/**
 * Represents the instruction in the action language. Each instruction can be associated with one or more labels as its index.
 * 
 * @author Yupeng
 * 
 */
public interface Instruction extends ExplanationPrinter
{
    /**
     * Returns the unique name of the instruction. Right now this name is just the Java class name.
     * 
     * @return the unique name of the instruction.
     */
    public String getName();
    
    /**
     * Gets the labels associated with the instruction.
     * 
     * @return the labels associated with the instruction.
     */
    Set<String> getLabels();
    
    /**
     * Adds one label to the instruction.
     * 
     * @param label
     *            the label to add
     */
    void addLabel(String label);
    
    /**
     * Executes the instruction.
     * 
     * @param uas
     *            the unified application state.
     * @return a label of the next instruction to run. Executes the next instruction in sequence if the label is <code>null</code>.
     * @throws FplInterpretationException
     *             any error occurs during action interpretation.
     */
    String execute(UnifiedApplicationState uas) throws FplInterpretationException;
}
