/**
 * 
 */
package edu.ucsd.forward.query.physical.dml;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.logical.dml.Update;
import edu.ucsd.forward.query.logical.dml.Update.Assignment;
import edu.ucsd.forward.query.physical.AbstractUnaryOperatorImpl;
import edu.ucsd.forward.query.physical.CopyContext;
import edu.ucsd.forward.query.physical.OperatorImpl;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * An abstract implementation of the update operator implementation interface.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractUpdateImpl extends AbstractUnaryOperatorImpl<Update> implements UpdateImpl
{
    @SuppressWarnings("unused")
    private static final Logger  log = Logger.getLogger(AbstractUpdateImpl.class);
    
    /**
     * The list of assignment implementations.
     */
    private List<AssignmentImpl> m_assignment_impl;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical update operator.
     * @param child
     *            the single child operator implementation.
     */
    public AbstractUpdateImpl(Update logical, OperatorImpl child)
    {
        super(logical);
        
        assert (child != null);
        this.addChild(child);
        
        m_assignment_impl = new ArrayList<AssignmentImpl>();
    }
    
    @Override
    public List<AssignmentImpl> getAssignmentImpls()
    {
        return m_assignment_impl;
    }
    
    /**
     * Adds an assignment implementation.
     * 
     * @param assignment_impl
     *            the assignment implementation.
     */
    public void addAssignmentImpl(AssignmentImpl assignment_impl)
    {
        assert assignment_impl != null;
        assert (this.getOperator().getAssignments().contains(assignment_impl.getAssignment()));
        m_assignment_impl.add(assignment_impl);
    }
    
    @Override
    public List<PhysicalPlan> getPhysicalPlansUsed()
    {
        List<PhysicalPlan> plans = new ArrayList<PhysicalPlan>();
        for (AssignmentImpl assignment_impl : m_assignment_impl)
        {
            plans.add(assignment_impl.getPhysicalPlan());
        }
        
        return plans;
    }
    
    /**
     * Represents an implementation of the assignment of update.
     * 
     * @author Yupeng
     * 
     */
    public final class AssignmentImpl
    {
        /**
         * The logical counterpart of the assignment implementation.
         */
        private Assignment   m_assignment;
        
        /**
         * The physical plan of the assignment implementation.
         */
        private PhysicalPlan m_plan;
        
        /**
         * Initializes an instance of an assignment implementation.
         * 
         * @param assignment
         *            the logical counterpart of the assignment implementation.
         * @param plan
         *            the physical plan of the assignment implementation.
         */
        public AssignmentImpl(Assignment assignment, PhysicalPlan plan)
        {
            assert assignment != null;
            assert plan != null;
            m_assignment = assignment;
            m_plan = plan;
        }
        
        /**
         * Gets the logical counterpart of the assignment implementation.
         * 
         * @return the logical counterpart of the assignment implementation.
         */
        public Assignment getAssignment()
        {
            return m_assignment;
        }
        
        /**
         * Returns the physical plan of the assignment implementation.
         * 
         * @return the physical plan of the assignment implementation.
         */
        public PhysicalPlan getPhysicalPlan()
        {
            return m_plan;
        }
        
        /**
         * Creates a copy of the assignment implementation.
         * 
         * @return a copied assignment implementation.
         */
        public AssignmentImpl copy(CopyContext context)
        {
            AssignmentImpl copy = new AssignmentImpl(m_assignment, m_plan.copy(context));
            
            return copy;
        }
    }
}
