/**
 * 
 */
package edu.ucsd.forward.data.constraint;

/**
 * A constraint checker checks that a data tree is consistent with respect to a constraint.
 * 
 * @author Kian Win
 * 
 */
public interface ConstraintChecker
{
    /**
     * Returns whether a data tree is consistent with respect to a constraint.
     * 
     * @return <code>true</code> if the data tree is consistent; <code>false</code> otherwise.
     */
    public boolean check();
}
