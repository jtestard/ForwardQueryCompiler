/**
 * 
 */
package edu.ucsd.forward.query.physical.dml;

import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.logical.dml.Insert;
import edu.ucsd.forward.query.physical.AbstractUnaryOperatorImpl;
import edu.ucsd.forward.query.physical.OperatorImpl;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * An abstract implementation of the insert operator implementation interface.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractInsertImpl extends AbstractUnaryOperatorImpl<Insert> implements InsertImpl
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(InsertImplInMemory.class);
    
    /**
     * The physical plan to insert.
     */
    private PhysicalPlan        m_insert_plan;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical insert operator.
     * @param child
     *            the single child operator implementation.
     */
    public AbstractInsertImpl(Insert logical, OperatorImpl child)
    {
        super(logical);
        
        assert child != null;
        addChild(child);
    }
    
    @Override
    public PhysicalPlan getInsertPlan()
    {
        return m_insert_plan;
    }
    
    /**
     * Sets the physical insert plan.
     * 
     * @param insert
     *            the physical insert plan.
     */
    public void setInsertPlan(PhysicalPlan insert)
    {
        assert (insert != null);
        m_insert_plan = insert;
    }
    
    @Override
    public List<PhysicalPlan> getPhysicalPlansUsed()
    {
        return Collections.<PhysicalPlan> singletonList(m_insert_plan);
    }
    
}
