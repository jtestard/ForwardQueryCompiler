/**
 * 
 */
package edu.ucsd.forward.query.physical.dml;

import edu.ucsd.forward.query.physical.UnaryOperatorImpl;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * The insert operator implementation interface.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface InsertImpl extends UnaryOperatorImpl
{
    /**
     * Gets the physical insert plan.
     * 
     * @return the physical insert plan.
     */
    public PhysicalPlan getInsertPlan();
    
}
