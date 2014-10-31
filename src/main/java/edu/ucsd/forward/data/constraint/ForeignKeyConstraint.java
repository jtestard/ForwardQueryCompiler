/**
 * 
 */
package edu.ucsd.forward.data.constraint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.source.SchemaObjectHandle;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeChecker;
import edu.ucsd.forward.data.value.Value;

/**
 * A foreign key constraint.
 * 
 * @author Kian Win
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class ForeignKeyConstraint extends AbstractConstraint
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ForeignKeyConstraint.class);
    
    private SchemaObjectHandle  m_foreign_handle;
    
    private CollectionType      m_foreign_collection_type;
    
    private List<ScalarType>    m_foreign_attributes;
    
    private SchemaObjectHandle  m_primary_handle;
    
    private CollectionType      m_primary_collection_type;
    
    private List<ScalarType>    m_primary_attributes;
    
    /**
     * Default constructor.
     */
    public ForeignKeyConstraint()
    {
        
    }
    
    /**
     * Constructs the constraint and adds it to both the given collection types.
     * 
     * @param foreign_handle
     *            - the foreign schema object handle.
     * @param foreign_collection_type
     *            - the foreign collection type.
     * @param foreign_attributes
     *            - the foreign attributes.
     * @param primary_handle
     *            - the primary schema object handle.
     * @param primary_collection_type
     *            - the primary collection type.
     * @param primary_attributes
     *            - the primary attributes.
     */
    public ForeignKeyConstraint(SchemaObjectHandle foreign_handle, CollectionType foreign_collection_type,
            List<ScalarType> foreign_attributes, SchemaObjectHandle primary_handle, CollectionType primary_collection_type,
            List<ScalarType> primary_attributes)
    {
        assert (foreign_handle != null);
        assert (foreign_collection_type != null);
        assert (foreign_attributes != null);
        assert (!foreign_attributes.isEmpty());
        
        assert (primary_handle != null);
        assert (primary_collection_type != null);
        assert (primary_attributes != null);
        assert (!primary_attributes.isEmpty());
        
        m_foreign_handle = foreign_handle;
        m_foreign_collection_type = foreign_collection_type;
        m_foreign_attributes = new ArrayList<ScalarType>(foreign_attributes);
        
        m_primary_handle = primary_handle;
        m_primary_collection_type = primary_collection_type;
        m_primary_attributes = new ArrayList<ScalarType>(primary_attributes);
        
        m_foreign_collection_type.addConstraint(this);
        if (m_foreign_collection_type != m_primary_collection_type)
        {
            m_primary_collection_type.addConstraint(this);
        }
        
        assert (TypeUtil.checkConstraintConsistent(m_foreign_collection_type));
    }
    
    @Override
    public List<Type> getAppliedTypes()
    {
        ArrayList<Type> ret = new ArrayList<Type>();
        ret.addAll(m_foreign_attributes);
        ret.addAll(m_primary_attributes);
        ret.add(m_foreign_collection_type);
        ret.add(m_primary_collection_type);
        return ret;
    }
    
    /**
     * Returns the foreign handle.
     * 
     * @return the foreign handle.
     */
    public SchemaObjectHandle getForeignHandle()
    {
        return m_foreign_handle;
    }
    
    /**
     * Returns the foreign collection type.
     * 
     * @return the foreign collection type.
     */
    public CollectionType getForeignCollectionType()
    {
        return m_foreign_collection_type;
    }
    
    /**
     * Returns the foreign attributes.
     * 
     * @return the foreign attributes.
     */
    public List<ScalarType> getForeignAttributes()
    {
        return Collections.unmodifiableList(m_foreign_attributes);
    }
    
    /**
     * Returns the primary handle.
     * 
     * @return the primary handle.
     */
    public SchemaObjectHandle getPrimaryHandle()
    {
        return m_primary_handle;
    }
    
    /**
     * Returns the primary collection type.
     * 
     * @return the primary collection type.
     */
    public CollectionType getPrimaryCollectionType()
    {
        return m_primary_collection_type;
    }
    
    /**
     * Returns the primary attributes.
     * 
     * @return the primary attributes.
     */
    public List<ScalarType> getPrimaryAttributes()
    {
        return Collections.unmodifiableList(m_primary_attributes);
    }
    
    @Override
    public Object[] getDeepEqualityObjects()
    {
        return new Object[] { m_foreign_collection_type, m_foreign_attributes, m_primary_collection_type, m_primary_attributes };
    }
    
    @Override
    public TypeChecker newTypeChecker(Type type)
    {
        return new ForeignKeyTypeChecker(this, type);
    }
    
    @Override
    public ConstraintChecker newConstraintChecker(Value value)
    {
        return new ForeignKeyConstraintChecker(this, value);
    }
    
    /**
     * Constructs a constraint checker for the constraint.
     * 
     * @author Yupeng
     * @author Michalis Petropoulos
     * 
     */
    private static class ForeignKeyConstraintChecker implements ConstraintChecker
    {
        private ForeignKeyConstraint m_constraint;
        
        private Value                m_value;
        
        /**
         * Constructs the constraint checker.
         * 
         * @param constraint
         *            the constraint.
         * @param value
         *            the value.
         */
        public ForeignKeyConstraintChecker(ForeignKeyConstraint constraint, Value value)
        {
            assert (constraint != null);
            assert (value != null);
            m_constraint = constraint;
            m_value = value;
        }
        
        @Override
        public boolean check()
        {
            assert (m_constraint != null);
            assert (m_value != null);
            
            // FIXME Add foreign key constraint data validation
            throw new UnsupportedOperationException();
        }
        
    }
    
    /**
     * Constructs a type checker for the constraint.
     * 
     * @author Kian Win
     * @author Michalis Petropoulos
     * 
     */
    private static class ForeignKeyTypeChecker implements TypeChecker
    {
        private ForeignKeyConstraint m_constraint;
        
        private Type                 m_type;
        
        /**
         * Constructs the type checker.
         * 
         * @param constraint
         *            the constraint.
         * @param type
         *            the type.
         */
        public ForeignKeyTypeChecker(ForeignKeyConstraint constraint, Type type)
        {
            assert (constraint != null);
            assert (type != null);
            m_constraint = constraint;
            m_type = type;
        }
        
        @Override
        public boolean check()
        {
            // The type is either the foreign or the primary one
            if (!m_constraint.getForeignCollectionType().hasSameTree(m_type)
                    && !m_constraint.getPrimaryCollectionType().hasSameTree(m_type)) return false;
            
            for (ScalarType attribute : m_constraint.getForeignAttributes())
            {
                // The collection type must be the closest collection-typed ancestor of the attribute
                if (attribute.getClosestAncestor(CollectionType.class) != m_constraint.getForeignCollectionType())
                {
                    return false;
                }
            }
            
            if (!m_constraint.getForeignCollectionType().getConstraints().contains(m_constraint)) return false;
            
            for (ScalarType attribute : m_constraint.getPrimaryAttributes())
            {
                // The collection type must be the closest collection-typed ancestor of the attribute
                if (attribute.getClosestAncestor(CollectionType.class) != m_constraint.getPrimaryCollectionType())
                {
                    return false;
                }
            }
            
            if (!m_constraint.getPrimaryCollectionType().getConstraints().contains(m_constraint)) return false;
            
            return true;
        }
    }
    
}
