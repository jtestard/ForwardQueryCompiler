/**
 * 
 */
package edu.ucsd.forward.data.constraint;

import java.io.Serializable;
import java.util.List;

import edu.ucsd.app2you.util.identity.DeepEquality;
import edu.ucsd.app2you.util.identity.Immutable;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.Value;

/**
 * A constraint.
 * 
 * @author Kian Win
 * @author Michalis Petropoulos
 * 
 */
public interface Constraint extends Immutable, DeepEquality, Serializable
{
    /**
     * Returns whether this constraint is type consistent with respect to a type.
     * 
     * @param type
     *            the type.
     * 
     * @return <code>true</code> if this constraint is type consistent; <code>false</code> otherwise.
     */
    public boolean isTypeConsistent(Type type);
    
    /**
     * Returns whether a value is constraint consistent with respect to this constraint.
     * 
     * @param value
     *            the value.
     * 
     * @return <code>true</code> if the value is consistent; <code>false</code> otherwise.
     */
    public boolean isConstraintConsistent(Value value);
    
    /**
     * Gets a <code>List</code> of the <code>Type</code>s that this <code>Constraint</code> applies to.
     * 
     * @return A <code>List</code> of the <code>Type</code>s on which this constraint acts.
     */
    public List<Type> getAppliedTypes();
    
}
