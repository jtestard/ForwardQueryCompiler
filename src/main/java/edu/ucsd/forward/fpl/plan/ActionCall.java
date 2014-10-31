/**
 * 
 */
package edu.ucsd.forward.fpl.plan;

import java.util.List;

import edu.ucsd.forward.fpl.ast.ActionInvocation.HttpVerb;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * Represents an action call in an FPL action.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface ActionCall extends Instruction
{
    /**
     * Gets the HTTP verb.
     * 
     * @return the HTTP verb.
     */
    public HttpVerb getHttpVerb();
    
    /**
     * Gets whether the action result will be displayed in a new browser window.
     * 
     * @return whether the action result will be displayed in a new browser window.
     */
    public boolean inNewWindow();
    
    /**
     * Gets the action call arguments.
     * 
     * @return the action call arguments.
     */
    public List<PhysicalPlan> getCallArguments();
    
}
