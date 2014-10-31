/**
 * 
 */
package edu.ucsd.forward.data.constraint;

import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeChecker;
import edu.ucsd.forward.data.value.Value;

/**
 * A singleton constraint.
 * 
 * @author Kian Win
 * 
 */
public class SingletonConstraint extends AbstractConstraint
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(SingletonConstraint.class);
    
    private CollectionType      m_coll_type;
    
    /**
     * Constructs the constraint.
     * 
     * @param relation_type
     *            - the relation type.
     */
    public SingletonConstraint(CollectionType relation_type)
    {
        assert (relation_type != null);
        
        m_coll_type = relation_type;
        relation_type.addConstraint(this);
    }
    
    @Override
    public Object[] getDeepEqualityObjects()
    {
        return new Object[] { m_coll_type };
    }
    
    @Override
    public List<Type> getAppliedTypes()
    {
        return Collections.<Type> singletonList(m_coll_type);
    }
    
    /**
     * Returns the relation type.
     * 
     * @return the relation type.
     */
    public CollectionType getCollectionType()
    {
        return m_coll_type;
    }
    
    @Override
    public TypeChecker newTypeChecker(Type type)
    {
        // FIXME
        return null;
    }
    
    @Override
    public ConstraintChecker newConstraintChecker(Value value)
    {
        // FIXME
        return null;
    }
}
