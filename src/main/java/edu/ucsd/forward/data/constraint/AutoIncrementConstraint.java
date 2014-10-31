/**
 * 
 */
package edu.ucsd.forward.data.constraint;

import java.util.Arrays;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeChecker;
import edu.ucsd.forward.data.value.Value;

/**
 * An auto-increment constraint.
 * 
 * @author Kian Win
 * @author Yupeng
 */
@SuppressWarnings("serial")
public class AutoIncrementConstraint extends AbstractConstraint
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AutoIncrementConstraint.class);
    
    private ScalarType          m_attribute;
    
    /**
     * Constructs the constraint.
     * 
     * @param attribute
     *            - the attribute.
     */
    public AutoIncrementConstraint(ScalarType attribute)
    {
        assert (attribute != null);
        
        m_attribute = attribute;
    }
    
    /**
     * Default constructor.
     */
    public AutoIncrementConstraint()
    {
        
    }
    
    @Override
    public Object[] getDeepEqualityObjects()
    {
        return new Object[] { m_attribute };
    }
    
    @Override
    public List<Type> getAppliedTypes()
    {
        return Arrays.asList(new Type[] { m_attribute });
    }
    
    /**
     * Returns the attribute.
     * 
     * @return the attribute.
     */
    public ScalarType getAttribute()
    {
        return m_attribute;
    }
    
    @Override
    public String toString()
    {
        String s = "Auto increment constraint:\n";
        s += "\nAttribute: " + new SchemaPath(m_attribute).toString() + ":" + m_attribute + "\n";
        return s;
    }
    
    @Override
    public TypeChecker newTypeChecker(Type type)
    {
        return new AutoIncrementTypeChecker(this, type);
    }
    
    @Override
    public ConstraintChecker newConstraintChecker(Value value)
    {
        // TODO Auto-generated method stub
        return null;
    }
    
    /**
     * Constructs a type checker for the constraint.
     * 
     * @author Kian Win
     * 
     */
    private static class AutoIncrementTypeChecker implements TypeChecker
    {
        private AutoIncrementConstraint m_constraint;
        
        private Type                    m_type;
        
        /**
         * Constructs the type checker.
         * 
         * @param constraint
         *            the constraint.
         * @param type
         *            the type.
         */
        public AutoIncrementTypeChecker(AutoIncrementConstraint constraint, Type type)
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
            
            // TODO: Also allow bigint
            if (!(m_constraint.getAttribute() instanceof IntegerType)) return false;
            
            return true;
        }
    }
    
}
