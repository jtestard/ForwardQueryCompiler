package edu.ucsd.forward.query.physical;

import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * Represents an operator implementation that contains a physical plan the query result of which is to be copied to a target data
 * source during execution. The nested physical plan does not have nested copy operator implementations. The copy plan operator
 * removes the intermediate results from the target data source when the physical plan is closing.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface CopyImpl extends OperatorImpl
{
    /**
     * Gets the physical plan to copy.
     * 
     * @return the physical plan to copy.
     */
    public PhysicalPlan getCopyPlan();
    
}
