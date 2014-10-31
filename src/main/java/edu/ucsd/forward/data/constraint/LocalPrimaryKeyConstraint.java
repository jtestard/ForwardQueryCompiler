/**
 * 
 */
package edu.ucsd.forward.data.constraint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.DataPath;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeChecker;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;

/**
 * A local primary key constraint.
 * 
 * @author Kian Win
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class LocalPrimaryKeyConstraint extends AbstractConstraint
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(LocalPrimaryKeyConstraint.class);
    
    private CollectionType      m_collection_type;
    
    private List<SchemaPath>    m_attributes_schema_paths;
    
    /**
     * Constructs the constraint and adds it to the given collection type.
     * 
     * @param collection_type
     *            - the collection type.
     * @param attributes
     *            - the attributes.
     */
    public LocalPrimaryKeyConstraint(CollectionType collection_type, List<? extends Type> attributes)
    {
        assert (collection_type != null);
        assert (attributes != null);
        assert (!attributes.isEmpty());
        
        m_collection_type = collection_type;
        m_collection_type.addConstraint(this);
        
        // Set the relative schema paths for the attributes
        List<SchemaPath> attributes_schema_paths = new ArrayList<SchemaPath>();
        for (Type attribute_type : attributes)
        {
            // FIXME Throw exception
            assert (attribute_type instanceof ScalarType);
            
            SchemaPath schema_path = new SchemaPath(m_collection_type, attribute_type);
            attributes_schema_paths.add(schema_path);
        }
        // unmodifiable list is not GWT serializable
        m_attributes_schema_paths = new ArrayList<SchemaPath>(attributes_schema_paths);        
    }

    /**
     * Default constructor.
     */
    public LocalPrimaryKeyConstraint()
    {
        
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
     * Returns the collection type schema path, computed on demand (not cached).
     * 
     * @return the collection type schema path.
     */
    public SchemaPath getCollectionTypeSchemaPath()
    {
        return new SchemaPath(m_collection_type);
    }
    
    /**
     * Returns the attributes.
     * 
     * @return the attributes.
     */
    public List<ScalarType> getAttributes()
    {
        List<ScalarType> attributes = new ArrayList<ScalarType>();
        for (SchemaPath path : this.m_attributes_schema_paths)
        {
            Type type = path.find(m_collection_type);
            attributes.add((ScalarType) type);
        }
        
        return attributes;
    }
    
    /**
     * Returns the relative schema paths of the attributes.
     * 
     * @return the relative schema paths of the attributes.
     */
    public List<SchemaPath> getAttributesSchemaPaths()
    {
        return new ArrayList<SchemaPath>(m_attributes_schema_paths);
    }
    
    @Override
    public List<Type> getAppliedTypes()
    {
        ArrayList<Type> ret = new ArrayList<Type>(this.getAttributes());
        ret.add(m_collection_type);
        
        return ret;
    }
    
    @Override
    public Object[] getDeepEqualityObjects()
    {
        return new Object[] { m_collection_type, m_attributes_schema_paths };
    }
    
    @Override
    public String toString()
    {
        String s = "Local primary key constraint:\n";
        s += "Collection: " + getCollectionTypeSchemaPath() + m_collection_type;
        s += "\nAttributes:{ ";
        for (SchemaPath path : m_attributes_schema_paths)
        {
            s += path + ";";
        }
        s += "}\n";
        return s;
    }
    
    @Override
    public TypeChecker newTypeChecker(Type type)
    {
        return new LocalPrimaryKeyTypeChecker(this, type);
    }
    
    @Override
    public ConstraintChecker newConstraintChecker(Value value)
    {
        return new LocalPrimaryKeyConstraintChecker(this, value);
    }
    
    /**
     * Constructs a constraint checker for the constraint.
     * 
     * @author Yupeng
     * 
     */
    private static class LocalPrimaryKeyConstraintChecker implements ConstraintChecker
    {
        private LocalPrimaryKeyConstraint m_constraint;
        
        private Value                     m_value;
        
        /**
         * Constructs the constraint checker.
         * 
         * @param constraint
         *            the constraint.
         * @param value
         *            the value.
         */
        public LocalPrimaryKeyConstraintChecker(LocalPrimaryKeyConstraint constraint, Value value)
        {
            assert (constraint != null);
            assert (value != null);
            m_constraint = constraint;
            m_value = value;
        }
        
        @Override
        public boolean check()
        {
            // // Does not check if the key is declared as auto increment.
            // List<ScalarType> key_types = m_constraint.getAttributes();
            // for (Constraint constraint : m_value.getType().getConstraints())
            // {
            // if (constraint instanceof AutoIncrementConstraint)
            // {
            // ScalarType scalar_type = ((AutoIncrementConstraint) constraint).getAttribute();
            // {
            // if (key_types.contains(scalar_type))
            // {
            // return true;
            // }
            // }
            // }
            // }
            
            // Handle null collections
            if (m_value instanceof NullValue) return true;
            
            assert (m_value instanceof CollectionValue);
            CollectionValue collection = (CollectionValue) m_value;
            
            Set<List<ScalarValue>> seen_key_values = new HashSet<List<ScalarValue>>();
            for (TupleValue tuple : collection.getTuples())
            {
                List<ScalarValue> key_value = new ArrayList<ScalarValue>();
                for (SchemaPath schema_path : m_constraint.getAttributesSchemaPaths())
                {
                    // Constraint schema paths are relative to the collection (i.e tuple/ex)
                    // The value here is a tuple, therefore we get the path steps 1 level
                    // relative from the root (i.e from tuple/ex to ex)
                    DataPath data_path = schema_path.toDataPath().relative(1);
                    Value value = data_path.find(tuple);
                    assert (value instanceof ScalarValue);
                    
                    key_value.add((ScalarValue) value);
                }
                
                for (List<ScalarValue> seen_key_value : seen_key_values)
                {
                    if (ValueUtil.deepEquals(key_value, seen_key_value))
                    {
                        // FIXME Throw a key violation exception
                        assert false : "key value" + key_value + "appears more than once.";
                        return false;
                    }
                }
                seen_key_values.add(key_value);
            }
            
            return true;
        }
        
    }
    
    /**
     * Constructs a type checker for the constraint.
     * 
     * @author Kian Win
     * @author Michalis Petropoulos
     * 
     */
    private static class LocalPrimaryKeyTypeChecker implements TypeChecker
    {
        private LocalPrimaryKeyConstraint m_constraint;
        
        private Type                      m_type;
        
        /**
         * Constructs the type checker.
         * 
         * @param constraint
         *            the constraint.
         * @param type
         *            the type.
         */
        public LocalPrimaryKeyTypeChecker(LocalPrimaryKeyConstraint constraint, Type type)
        {
            assert (constraint != null);
            assert (type != null);
            m_constraint = constraint;
            m_type = type;
        }
        
        @Override
        public boolean check()
        {
            if (!m_constraint.getCollectionType().hasSameTree(m_type)) return false;
            
            for (ScalarType attribute : m_constraint.getAttributes())
            {
                // The collection type must be the closest collection-typed ancestor of the attribute
                if (attribute.getClosestAncestor(CollectionType.class) != m_constraint.getCollectionType())
                {
                    return false;
                }
            }
            
            if (m_constraint.getCollectionType().getLocalPrimaryKeyConstraint() != m_constraint) return false;
            
            return true;
        }
    }
    
}
