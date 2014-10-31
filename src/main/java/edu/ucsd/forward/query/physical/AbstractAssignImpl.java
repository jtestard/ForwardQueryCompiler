/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * @author Vicky Papavasileiou
 *
 */
public abstract class AbstractAssignImpl  extends AbstractOperatorImpl<Assign> implements AssignImpl
{
    private static final long                 serialVersionUID = 1L;
    
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractAssignImpl.class);
    
    private PhysicalPlan    m_assign_plan;
        
    
    public AbstractAssignImpl(Assign a)
    {
        super(a);
    }
    
    public PhysicalPlan getAssignPlan()
    {
        return m_assign_plan;
    }
    
    /**
     * Sets the assign plan.
     * 
     * @param plan
     *            the plan to assign.
     */
    public void setAssignPlan(PhysicalPlan plan)
    {
        assert plan != null;
        m_assign_plan = plan;
    }
    
    @Override
    public List<PhysicalPlan> getPhysicalPlansUsed()
    {
        return Collections.<PhysicalPlan> singletonList(m_assign_plan);
    }
    
    abstract protected void addReference(ScanImplReferencing reference);
    
    abstract protected Binding getNext(ScanImplReferencing reference) throws QueryExecutionException, SuspensionException;
}
