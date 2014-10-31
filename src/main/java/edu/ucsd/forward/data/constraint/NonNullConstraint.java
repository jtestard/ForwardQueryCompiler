/**
 * 
 */
package edu.ucsd.forward.data.constraint;

import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeChecker;
import edu.ucsd.forward.data.value.Value;

/**
 * A non-null constraint.
 * 
 * @author Kian Win
 * @author Yupeng
 */
public class NonNullConstraint extends AbstractConstraint
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(NonNullConstraint.class);
    
    private Type                m_attribute;
    
    /**
     * Constructs the constraint and adds it to the given type.
     * 
     * @param attribute
     *            - the attribute.
     */
    public NonNullConstraint(Type attribute)
    {
        assert (attribute != null);
        
        m_attribute = attribute;
        
        m_attribute.addConstraint(this);
    }
    
    /**
     * Returns the attribute.
     * 
     * @return the attribute.
     */
    public Type getAttribute()
    {
        return m_attribute;
    }
    
    @Override
    public TypeChecker newTypeChecker(Type type)
    {
        return new NonNullTypeChecker(this, type);
    }
    
    @Override
    public ConstraintChecker newConstraintChecker(Value value)
    {
        // FIXME
        throw new AssertionError();
    }
    
    @Override
    public List<Type> getAppliedTypes()
    {
        return Collections.singletonList(m_attribute);
    }
    
    @Override
    public Object[] getDeepEqualityObjects()
    {
        return new Object[] { m_attribute };
    }
    
    @Override
    public String toString()
    {
        return "NonNullConstraint: " + new SchemaPath(m_attribute) + " is not null\n";
    }
    
    /**
     * Constructs a type checker for the constraint.
     * 
     * @author Michalis Petropoulos
     * 
     */
    private static class NonNullTypeChecker implements TypeChecker
    {
        private NonNullConstraint m_constraint;
        
        private Type              m_type;
        
        /**
         * Constructs the type checker.
         * 
         * @param constraint
         *            the constraint.
         * @param type
         *            the type.
         */
        public NonNullTypeChecker(NonNullConstraint constraint, Type type)
        {
            assert (constraint != null);
            assert (type != null);
            m_constraint = constraint;
            m_type = type;
        }
        
        @Override
        public boolean check()
        {
            if (!m_constraint.getAttribute().hasSameTree(m_type)) return false;
            
            return true;
        }
        
    }
    
}
