/**
 * 
 */
package edu.ucsd.forward.query.physical;

import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * @author Vicky Papavasileiou
 *
 */
public interface AssignImpl extends OperatorImpl
{
    public PhysicalPlan getAssignPlan();
    
    public void setAssignPlan(PhysicalPlan plan);
}
