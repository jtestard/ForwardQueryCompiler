/**
 * 
 */
package edu.ucsd.forward.data.constraint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeChecker;
import edu.ucsd.forward.data.value.Value;

/**
 * A homogeneous constraint.
 * 
 * @author Kian Win
 * 
 */
public class HomogeneousConstraint extends AbstractConstraint
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(HomogeneousConstraint.class);
    
    private TupleType           m_tuple_type;
    
    private List<Type>          m_attributes;
    
    /**
     * Constructs the constraint.
     * 
     * @param tuple_type
     *            - the tuple type.
     * @param attributes
     *            - the attributes.
     */
    public HomogeneousConstraint(TupleType tuple_type, List<Type> attributes)
    {
        assert (tuple_type != null);
        assert (attributes != null);
        assert (!attributes.isEmpty());
        
        m_tuple_type = tuple_type;
        m_attributes = attributes;
    }
    
    @Override
    public Object[] getDeepEqualityObjects()
    {
        return new Object[] { m_tuple_type, m_attributes };
    }
    
    @Override
    public List<Type> getAppliedTypes()
    {
        ArrayList<Type> ret = new ArrayList<Type>(m_attributes);
        ret.add(m_tuple_type);
        return ret;
    }
    
    /**
     * Returns the tuple type.
     * 
     * @return the tuple type.
     */
    public TupleType getTupleType()
    {
        return m_tuple_type;
    }
    
    /**
     * Returns the attributes.
     * 
     * @return the attributes.
     */
    public List<Type> getAttributes()
    {
        return Collections.unmodifiableList(m_attributes);
    }
    
    @Override
    public TypeChecker newTypeChecker(Type type)
    {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public ConstraintChecker newConstraintChecker(Value value)
    {
        // TODO Auto-generated method stub
        return null;
    }
    
}
