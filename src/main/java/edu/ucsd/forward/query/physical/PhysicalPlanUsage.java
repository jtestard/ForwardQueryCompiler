/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.List;

import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * The interface to detect the list of physical plan used by an operator implementation as arguments.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface PhysicalPlanUsage
{
    /**
     * Returns the list of physical plan used by an operator implementation as arguments.
     * 
     * @return the list of physical plan used by an operator implementation as arguments.
     */
    public List<PhysicalPlan> getPhysicalPlansUsed();
    
}
