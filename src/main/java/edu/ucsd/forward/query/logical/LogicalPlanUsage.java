/**
 * 
 */
package edu.ucsd.forward.query.logical;

import java.util.List;

import edu.ucsd.forward.query.logical.plan.LogicalPlan;

/**
 * The interface to detect the list of logical plan used by an operator as arguments.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface LogicalPlanUsage
{
    /**
     * Returns the list of logical plan used by an operator as arguments.
     * 
     * @return the list of logical plan used by an operator as arguments.
     */
    public List<LogicalPlan> getLogicalPlansUsed();
    
}
