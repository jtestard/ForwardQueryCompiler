package edu.ucsd.forward.query.physical;

import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.query.logical.Copy;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * Represents an abstract implementation of the copy interface that copies the query result of a physical plan to a target data
 * source.
 * 
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractCopyImpl extends AbstractOperatorImpl<Copy> implements CopyImpl
{
    /**
     * The physical plan to copy.
     */
    private PhysicalPlan m_copy_plan;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical copy plan operator.
     */
    public AbstractCopyImpl(Copy logical)
    {
        super(logical);
    }
    
    /**
     * Sets the physical plan to copy.
     * 
     * @param copy
     *            the physical plan to copy.
     */
    public void setCopyPlan(PhysicalPlan copy)
    {
        assert (copy != null);
        m_copy_plan = copy;
    }
    
    @Override
    public PhysicalPlan getCopyPlan()
    {
        return m_copy_plan;
    }
    
    @Override
    public List<PhysicalPlan> getPhysicalPlansUsed()
    {
        return Collections.<PhysicalPlan> singletonList(m_copy_plan);
    }
    
}
