/**
 * 
 */
package edu.ucsd.forward.data.constraint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeChecker;
import edu.ucsd.forward.data.value.Value;

/**
 * A global uniqueness constraint.
 * 
 * @author Kian Win
 * 
 */
public class GlobalUniquenessConstraint extends AbstractConstraint
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(GlobalUniquenessConstraint.class);
    
    private CollectionType      m_relation_type;
    
    private List<Type>          m_attributes;
    
    /**
     * Constructs the constraint.
     * 
     * @param relation_type
     *            - the relation type.
     * @param attributes
     *            - the attributes.
     */
    public GlobalUniquenessConstraint(CollectionType relation_type, List<Type> attributes)
    {
        assert (relation_type != null);
        assert (attributes != null);
        assert (!attributes.isEmpty());
        
        m_relation_type = relation_type;
        m_attributes = attributes;
    }
    
    @Override
    public Object[] getDeepEqualityObjects()
    {
        return new Object[] { m_relation_type, m_attributes };
    }
    
    @Override
    public List<Type> getAppliedTypes()
    {
        ArrayList<Type> ret = new ArrayList<Type>(m_attributes);
        ret.add(m_relation_type);
        return ret;
    }
    
    /**
     * Returns the relation type.
     * 
     * @return the relation type.
     */
    public CollectionType getRelationType()
    {
        return m_relation_type;
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
