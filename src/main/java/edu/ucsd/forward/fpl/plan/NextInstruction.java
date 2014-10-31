/**
 * 
 */
package edu.ucsd.forward.fpl.plan;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;

/**
 * An instruction that does nothing but act as a place-holder.
 * 
 * @author Yupeng
 * 
 */
public class NextInstruction extends AbstractInstruction
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(NextInstruction.class);
    
    /**
     * Constructs the null instruction with a label.
     * 
     * @param label
     *            the label associated with the instruction.
     */
    public NextInstruction(String label)
    {
        this.addLabel(label);
    }
    
    /**
     * Default constructor.
     */
    public NextInstruction()
    {
        
    }
    
    @Override
    public String toExplainString()
    {
        String str = super.toExplainString();
        return str + " NEXT " + "\n";
    }
    
    @Override
    public String execute(UnifiedApplicationState uas)
    {
        // Do nothing
        return null;
    }
}
