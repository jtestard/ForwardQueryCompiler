package edu.ucsd.forward.query.physical;

import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * Represents an operator implementation that contains a physical plan to be sent to a target data source for execution. The nested
 * physical plan does not have nested send plan operator implementations. Instead, the send plan operator implementation has child
 * copy operator implementations that copy intermediate results to the target data source. Then, the contained physical plan is sent
 * to the target data source for execution.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface SendPlanImpl extends OperatorImpl
{
    /**
     * Gets the physical plan to send.
     * 
     * @return the physical plan to send.
     */
    public PhysicalPlan getSendPlan();
    
}
