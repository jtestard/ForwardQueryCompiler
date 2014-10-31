/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.HashMap;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * The context in the copy of operator implementations and physical plan. This is useful when copy the physical plan as a graph.
 * 
 * @author Yupeng Fu
 * 
 */
public class CopyContext
{
    @SuppressWarnings("unused")
    private static final Logger     log = Logger.getLogger(CopyContext.class);
    
    /**
     * The list of assigned targets seen in the context.
     */
    private Map<String, AbstractAssignImpl> m_target_assign_map;

    
    /**
     * Constructor.
     */
    public CopyContext()
    {
        m_target_assign_map = new HashMap<String, AbstractAssignImpl>();
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
