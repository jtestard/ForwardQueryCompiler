/**
 * 
 */
package edu.ucsd.forward.data.constraint;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.value.DataTree;

/**
 * Abstract class for implementing constraint checkers.
 * 
 * @author Kian Win
 * 
 */
public class AbstractConstraintChecker
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractConstraintChecker.class);
    
    private DataTree            m_data_tree;
    
    private Constraint          m_constraint;
    
    /**
     * Constructs a constraint checker.
     * 
     * @param data_tree
     *            - the data tree to check.
     * @param constraint
     *            - the constraint.
     * 
     */
    public AbstractConstraintChecker(DataTree data_tree, Constraint constraint)
    {
        assert (data_tree.isTypeConsistent());
        m_data_tree = data_tree;
    }
    
    /**
     * Returns the data tree to check.
     * 
     * @return the data tree to check.
     */
    public DataTree getDataTree()
    {
        return m_data_tree;
    }
    
    /**
     * Returns the constraint.
     * 
     * @return the constraint.
     */
    public Constraint getConstraint()
    {
        return m_constraint;
    }
    
}
