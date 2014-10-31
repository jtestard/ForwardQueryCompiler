/**
 * 
 */
package edu.ucsd.forward.data.constraint;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeChecker;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.TupleValueComparator;
import edu.ucsd.forward.data.value.Value;

/**
 * An order constraint that specifies the orders of the tuples in a relation.
 * 
 * @author Yupeng
 * 
 */
public class OrderConstraint extends AbstractConstraint
{
    private static final long      serialVersionUID = 1L;
    
    @SuppressWarnings("unused")
    private static final Logger    log              = Logger.getLogger(OrderConstraint.class);
    
    private CollectionType         m_collection_type;
    
    private List<OrderByAttribute> m_attributes;
    
    /**
     * Constructs the relation type.
     * 
     * @param collection_type
     *            the collection type
     * @param attributes
     *            the order by attributes
     */
    public OrderConstraint(CollectionType collection_type, List<OrderByAttribute> attributes)
    {
        assert (collection_type != null) : "The relation type cannot be null.";
        assert (attributes != null) : "The order by attributes cannot be null.";
        
        m_collection_type = collection_type;
        m_attributes = attributes;
    }
    
    @Override
    public ConstraintChecker newConstraintChecker(Value value)
    {
        return new OrderConstraintChecker(this, value);
    }
    
    @Override
    public TypeChecker newTypeChecker(Type type)
    {
        return new OrderConstraintTypeChecker(this, type);
    }
    
    @Override
    public List<Type> getAppliedTypes()
    {
        List<Type> ret = new ArrayList<Type>();
        for (OrderByAttribute attribute : m_attributes)
        {
            ret.add(attribute.getAttribute());
        }
        ret.add(m_collection_type);
        return ret;
    }
    
    /**
     * Returns the collection type.
     * 
     * @return the collection type.
     */
    public CollectionType getCollectionType()
    {
        return m_collection_type;
    }
    
    /**
     * Returns the order by attributes.
     * 
     * @return the order by attributes.
     */
    public List<OrderByAttribute> getOrderByAttributes()
    {
        return m_attributes;
    }
    
    /**
     * Adds one attribute.
     * 
     * @param attribute
     *            the attribute
     */
    public void addOrderByAttribute(OrderByAttribute attribute)
    {
        m_attributes.add(attribute);
    }
    
    @Override
    public Object[] getDeepEqualityObjects()
    {
        return new Object[] { m_collection_type, m_attributes };
    }
    
    /**
     * Constructs a constraint checker for the constraint.
     * 
     * @author Yupeng
     * 
     */
    public static class OrderConstraintChecker implements ConstraintChecker
    {
        private OrderConstraint      m_constraint;
        
        private Value                m_value;
        
        private TupleValueComparator m_comparator;
        
        /**
         * Construct the order constraint checker.
         * 
         * @param constraint
         *            the order constraint.
         * @param value
         *            the value to check.
         */
        public OrderConstraintChecker(OrderConstraint constraint, Value value)
        {
            assert (constraint != null);
            assert (value != null);
            m_constraint = constraint;
            m_value = value;
            m_comparator = new TupleValueComparator(m_constraint);
        }
        
        /**
         * <b>Inherited JavaDoc:</b><br>
         * {@inheritDoc} <br>
         * <br>
         * <b>See original method below.</b> <br>
         * 
         * @see edu.ucsd.forward.data.constraint.ConstraintChecker#check()
         */
        @Override
        public boolean check()
        {
            if (m_value instanceof NullValue) return true;
            
            assert (m_value instanceof CollectionValue);
            CollectionValue collection = (CollectionValue) m_value;
            
            TupleValue prev = null;
            
            for (TupleValue tuple : collection.getTuples())
            {
                if (prev != null && m_comparator.compare(prev, tuple) > 0) return false;
                prev = tuple;
            }
            
            return true;
        }
    }
    
    /**
     * Constructs a type checker for the constraint.
     * 
     * @author Yupeng
     * 
     */
    public static class OrderConstraintTypeChecker implements TypeChecker
    {
        private OrderConstraint m_constraint;
        
        private Type            m_type;
        
        /**
         * Constructs the type checker.
         * 
         * @param constraint
         *            the constraint.
         * @param type
         *            the type.
         */
        public OrderConstraintTypeChecker(OrderConstraint constraint, Type type)
        {
            m_constraint = constraint;
            m_type = type;
        }
        
        @Override
        public boolean check()
        {
            if (!m_constraint.getCollectionType().hasSameTree(m_type)) return false;
            
            for (OrderByAttribute order_by_attribute : m_constraint.getOrderByAttributes())
            {
                Type attribute = order_by_attribute.getAttribute();
                if (!attribute.hasSameTree(m_type)) return false;
                
                if (attribute.getParent() != m_constraint.getCollectionType().getTupleType()) return false;
            }
            
            // FIXME: check other restrictions
            return true;
        }
    }
    
}
