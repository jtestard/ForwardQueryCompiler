/**
 * 
 */
package edu.ucsd.forward.query.logical.visitors;

import java.util.HashMap;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.physical.AbstractAssignImpl;
import edu.ucsd.forward.query.physical.AssignMemoryToMemoryImpl;

/**
 * The context of a physical plan builder consists of a map of assign operator implementations created for the assigns encountered
 * in the logical plan, mapped from the unique assign target name.
 * 
 * @author Yupeng Fu
 * 
 */
public final class PhysicalPlanBuilderContext
{
    @SuppressWarnings("unused")
    private static final Logger     log = Logger.getLogger(PhysicalPlanBuilderContext.class);
    
    private PhysicalPlanBuilder     m_builder;
    
    /**
     * The list of assigned targets seen in the context.
     */
    private Map<String, AbstractAssignImpl> m_target_assign_map;
    
    /**
     * Constructor.
     * 
     * @param builder
     *            the physical plan builder.
     */
    protected PhysicalPlanBuilderContext(PhysicalPlanBuilder builder)
    {
        assert builder != null;
        m_builder = builder;
        m_target_assign_map = new HashMap<String, AbstractAssignImpl>();
    }
    
    /**
     * Gets the physical plan builder.
     * 
     * @return the physical plan builder.
     */
    protected PhysicalPlanBuilder getPhysicalPlanBuilder()
    {
        return m_builder;
    }
    
    /**
     * Adds an assign target to the context.
     * 
     * @param target
     *            the target of an assign operator.
     * @param assign_impl
     *            the assign operator implementation.
     */
    protected void addAssignTarget(String target, AbstractAssignImpl assign_impl)
    {
        assert target != null;
        assert assign_impl != null;
        assert !m_target_assign_map.containsKey(target);
        m_target_assign_map.put(target, assign_impl);
    }
    
    /**
     * Checks if the assign target is already contained in the context.
     * 
     * @param target
     *            the target to test
     * @return whether the assign target is already contained in the context.
     */
    protected boolean containsAssignTarget(String target)
    {
        assert target != null;
        return m_target_assign_map.containsKey(target);
    }
    
    /**
     * Gets the assign operator implementation by giving its target name.
     * 
     * @param target
     *            the target
     * @return the assign operator implementation.
     */
    protected AbstractAssignImpl getAssgin(String target)
    {
        assert target != null;
        return m_target_assign_map.get(target);
    }
}
