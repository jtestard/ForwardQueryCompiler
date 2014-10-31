/**
 * 
 */
package edu.ucsd.forward.data.constraint;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeChecker;
import edu.ucsd.forward.data.value.Value;

/**
 * Abstract class for implementing constraints.
 * 
 * @author Kian Win
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public abstract class AbstractConstraint implements Constraint
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractConstraint.class);
    
    protected AbstractConstraint()
    {
        
    }
    
    @Override
    public boolean isTypeConsistent(Type type)
    {
        assert (type != null);
        return newTypeChecker(type).check();
    }
    
    @Override
    public boolean isConstraintConsistent(Value value)
    {
        assert (value != null);
        return newConstraintChecker(value).check();
    }
    
    /**
     * Returns a new type checker instance that checks whether the constraint is type consistent with respect to a type.
     * 
     * @param type
     *            - the type.
     * @return a new type checker instance.
     */
    public abstract TypeChecker newTypeChecker(Type type);
    
    /**
     * Returns a new constraint checker instance that checks whether a value is constraint consistent with respect to the
     * constraint.
     * 
     * @param value
     *            - the value to check.
     * 
     * @return a new constraint checker instance.
     */
    public abstract ConstraintChecker newConstraintChecker(Value value);
}
