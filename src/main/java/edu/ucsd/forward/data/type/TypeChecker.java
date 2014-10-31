/**
 * 
 */
package edu.ucsd.forward.data.type;

/**
 * A typechecker checks for type consistency.
 * 
 * @author Kian Win
 * 
 */
public interface TypeChecker
{
    /**
     * Checks for type consistency.
     * 
     * @return <code>true</code> if the checked data structures are type consistent; <code>false</code> otherwise.
     */
    public boolean check();
}
