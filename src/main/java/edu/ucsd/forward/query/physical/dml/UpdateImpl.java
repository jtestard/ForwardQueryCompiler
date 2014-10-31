/**
 * 
 */
package edu.ucsd.forward.query.physical.dml;

import java.util.List;

import edu.ucsd.forward.query.physical.UnaryOperatorImpl;
import edu.ucsd.forward.query.physical.dml.AbstractUpdateImpl.AssignmentImpl;

/**
 * The update operator implementation interface.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface UpdateImpl extends UnaryOperatorImpl
{
    /**
     * Gets the list of the assignment implementations.
     * 
     * @return the list of the assignment implementations.
     */
    public List<AssignmentImpl> getAssignmentImpls();
    
}
