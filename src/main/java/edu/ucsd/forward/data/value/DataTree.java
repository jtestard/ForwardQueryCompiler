/**
 * 
 */
package edu.ucsd.forward.data.value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.constraint.Constraint;
import edu.ucsd.forward.data.constraint.ConstraintChecker;
import edu.ucsd.forward.data.constraint.LocalPrimaryKeyConstraint;
import edu.ucsd.forward.data.constraint.OrderConstraint;
import edu.ucsd.forward.data.index.Index;
import edu.ucsd.forward.data.index.IndexDeclaration;
import edu.ucsd.forward.data.index.IndexMethod;
import edu.ucsd.forward.data.index.IndexRedBlack;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.DoubleType;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.SwitchType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.TupleValue.AttributeValueEntry;
import edu.ucsd.forward.exception.ExceptionMessages.DataSource;

/**
 * A data tree contains a root value.
 * 
 * @author Kian Win
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class DataTree extends AbstractComplexValue<SchemaTree>
{
    @SuppressWarnings("unused")
    private static final Logger                     log = Logger.getLogger(DataTree.class);
    
    private Value                                   m_root_value;
    
    private boolean                                 m_type_consistent;
    
    private boolean                                 m_constraint_consistent;
    
    private boolean                                 m_deep_immutable;
    
    private Map<String, Index>                      m_indices;
    
    /**
     * A pointer to the containing data source.
     */
    private edu.ucsd.forward.data.source.DataSource m_data_source;
    
    /**
     * Default constructor.
     */
    protected DataTree()
    {
        
    }
    
    /**
     * Constructs a data tree.
     * 
     * @param root_value
     *            - the root value.
     */
    public DataTree(Value root_value)
    {
        setRootValue(root_value);
        m_indices = new HashMap<String, Index>();
    }
    
    /**
     * Returns the root value.
     * 
     * @return the root value.
     */
    public Value getRootValue()
    {
        return m_root_value;
    }
    
    public void setDataSource(edu.ucsd.forward.data.source.DataSource source)
    {
        m_data_source = source;
    }
    
    public edu.ucsd.forward.data.source.DataSource getDataSource()
    {
        return m_data_source;
    }
    
    /**
     * Sets the root value. Setting the root value to <code>null</code> will detach the existing root value (if any).
     * 
     * @param root_value
     *            the root value.
     */
    @SuppressWarnings("unchecked")
    public void setRootValue(Value root_value)
    {
        // Detach the old root value
        if (m_root_value != null)
        {
            ((AbstractValue) m_root_value).setParent(null);
        }
        
        // Attach the new root value
        m_root_value = root_value;
        if (m_root_value != null)
        {
            ((AbstractValue) m_root_value).setParent(this);
        }
    }
    
    @Override
    public Class<SchemaTree> getTypeClass()
    {
        return SchemaTree.class;
    }
    
    /**
     * Creates an index on the data tree.
     * 
     * @param declaration
     *            the declaration of the index.
     * @throws DataSourceException
     *             if the index already exists
     */
    public void createIndex(IndexDeclaration declaration) throws DataSourceException
    {
        String full_name = declaration.getFullName();
        if (m_indices.containsKey(full_name))
        {
            throw new DataSourceException(DataSource.INVALID_NEW_INDEX_NAME, declaration.getName(),
                                          declaration.getCollectionTypeSchemaPath().toString());
        }
        // Build the index
        IndexMethod method = declaration.getMethod();
        Index index = null;
        if (method == IndexMethod.BTREE)
        {
            index = new IndexRedBlack(declaration);
        }
        else
        {
            throw new UnsupportedOperationException();
        }
        // FIXME: For now the index can only be built on the root collection
        assert declaration.getCollectionTypeSchemaPath().getLength() == 0;
        CollectionValue collection = (CollectionValue) getRootValue();
        index.load(collection);
        m_indices.put(full_name, index);
    }
    
    /**
     * Deletes a given index.
     * 
     * @param path_to_coll
     *            the path to the collection.
     * @param name
     *            the name of the index.
     * @throws DataSourceException
     *             if the index to delete does not exist
     */
    public void deleteIndex(SchemaPath path_to_coll, String name) throws DataSourceException
    {
        String full_name = path_to_coll + IndexDeclaration.SEPARATION + name;
        if (!m_indices.containsKey(full_name))
        {
            throw new DataSourceException(DataSource.UNKNOWN_INDEX_NAME, path_to_coll.toString(), name);
        }
        m_indices.remove(full_name);
    }
    
    /**
     * Gets the specified index.
     * 
     * @param path_to_coll
     *            the path to the collection.
     * @param name
     *            the name of the index.
     * @return the found index.
     * @throws DataSourceException
     *             if the index does not exist.
     */
    public Index getIndex(SchemaPath path_to_coll, String name) throws DataSourceException
    {
        String full_name = path_to_coll + IndexDeclaration.SEPARATION + name;
        if (!m_indices.containsKey(full_name))
        {
            throw new DataSourceException(DataSource.NON_EXISTING_INDEX, name, path_to_coll.toString());
        }
        return m_indices.get(full_name);
    }
    
    /**
     * Gets the indices on a collection.
     * 
     * @param path_to_coll
     *            the path to the collection.
     * @return the found indices.
     */
    public List<Index> getIndices(SchemaPath path_to_coll)
    {
        String prefix = path_to_coll + IndexDeclaration.SEPARATION;
        List<Index> indices = new ArrayList<Index>();
        for (String name : m_indices.keySet())
        {
            if (name.startsWith(prefix)) indices.add(m_indices.get(name));
        }
        
        return indices;
    }
    
    @Override
    @Deprecated
    public SchemaTree getType()
    {
        // Override super method to give more specific return signature
        return (SchemaTree) super.getType();
    }
    
    @Override
    protected void setParent(AbstractComplexValue<?> parent)
    {
        // A data tree can only be the root node
        assert parent == null;
        super.setParent(parent);
    }
    
    @Override
    public List<Value> getChildren()
    {
        return Arrays.asList(m_root_value);
    }
    
    /**
     * Returns whether the data tree is type consistent.
     * 
     * @return <code>true</code> if the data tree is type consistent; <code>false</code> otherwise.
     */
    public boolean isTypeConsistent()
    {
        return m_type_consistent;
    }
    
    /**
     * Checks whether the data tree is consistent.
     * 
     * @return <code>true</code> if the data tree is consistent; <code>false</code> otherwise.
     */
    public boolean checkConsistent()
    {
        boolean consistent = true;
        if (!checkTypeConsistent()) consistent = false;
        if (!checkConstraintConsistent()) consistent = false;
        return consistent;
    }
    
    /**
     * Returns whether the data tree is consistent.
     * 
     * @return <code>true</code> if the data tree is consistent; <code>false</code> otherwise.
     */
    public boolean isConsistent()
    {
        return isTypeConsistent() && isConstraintConsistent();
    }
    
    /**
     * Checks whether the data tree is type consistent.
     * 
     * @return <code>true</code> if the data tree is type consistent; <code>false</code> otherwise.
     */
    public boolean checkTypeConsistent()
    {
        assert !isDeepImmutable();
        
        checkTypeConsistent(this);
        
        m_type_consistent = true;
        
        return isTypeConsistent();
    }
    
    /**
     * Checks the type consistency of the data tree and its associated type.
     * 
     * @param value
     *            the root of the data tree
     */
    private void checkTypeConsistent(Value value)
    {
        Type type = value.getType();
        assert type != null : "The associated type cannot be null.";
        if (value.getParent() != null)
        {
            // Check the parent type is the same as the type associated with the parent value.
            assert type.getParent() == value.getParent().getType() : "The parent type must be the same as the type associated with the parent value";
            // FIXME if the parent is tuple, then type should also be checked to have the correct attribute name, likewise for
            // switch value.
        }
        else if (value instanceof DataTree)
        {
            assert type instanceof SchemaTree : "The type associated with data tree must be schema tree.";
            checkTypeConsistent(((DataTree) value).getRootValue());
        }
        else if (value instanceof CollectionValue)
        {
            assert type instanceof CollectionType : "The type associated with relation must be collection type.";
            for (TupleValue tuple : ((CollectionValue) value).getTuples())
            {
                checkTypeConsistent(tuple);
            }
            // FIXME we should check that all tuples have the same attributes.
        }
        else if (value instanceof TupleValue)
        {
            assert type instanceof TupleType : "The type associated with tuple must be tuple type.";
            for (AttributeValueEntry entry : (TupleValue) value)
            {
                checkTypeConsistent(entry.getValue());
            }
        }
        else if (value instanceof SwitchValue)
        {
            assert type instanceof SwitchType : "The type associated with switch value must be switch type.";
            checkTypeConsistent(((SwitchValue) value).getCase());
        }
        else if (value instanceof StringValue)
        {
            assert type instanceof StringType : "The type associated with string value must be string type.";
        }
        else if (value instanceof IntegerValue)
        {
            assert type instanceof IntegerType : "The type associated with integer value must be integer type.";
        }
        else if (value instanceof BooleanValue)
        {
            assert type instanceof BooleanType : "The type associated with boolean value must be boolean type.";
        }
        else if (value instanceof DoubleValue)
        {
            assert type instanceof DoubleType : "The type associated with double value must be double type.";
        }
    }
    
    /**
     * Resets the type consistency.
     * 
     */
    private void resetTypeConsistent()
    {
        assert (!isDeepImmutable());
        m_type_consistent = false;
    }
    
    /**
     * Sets the type consistency.
     * 
     * @param type_consistent
     *            whether the data tree is type consistent.
     */
    public void setTypeConsistent(boolean type_consistent)
    {
        assert (!isDeepImmutable());
        m_type_consistent = type_consistent;
    }
    
    /**
     * Returns whether the data tree is constraint consistent.
     * 
     * @return <code>true</code> if the data tree is constraint consistent; <code>false</code> otherwise.
     */
    public boolean isConstraintConsistent()
    {
        return m_constraint_consistent;
    }
    
    /**
     * Checks whether the data tree is constraint consistent.
     * 
     * @return <code>true</code> if the data tree is constraint consistent; <code>false</code> otherwise.
     */
    public boolean checkConstraintConsistent()
    {
        assert !isDeepImmutable();
        assert getRootValue() != null;
        m_constraint_consistent = true;
        
        for (CollectionValue collection_value : getRootValue().getDescendants(CollectionValue.class))
        {
            CollectionType collection_type = collection_value.getType();
            
            assert (collection_type != null) : "The data tree was not properly mapped to a schema tree";
            
            for (Constraint constraint : collection_type.getConstraints())
            {
                if (constraint instanceof LocalPrimaryKeyConstraint)
                {
                    ConstraintChecker checker = ((LocalPrimaryKeyConstraint) constraint).newConstraintChecker(collection_value);
                    if (!checker.check()) m_constraint_consistent = false;
                }
                if (constraint instanceof OrderConstraint)
                {
                    ConstraintChecker checker = ((OrderConstraint) constraint).newConstraintChecker(collection_value);
                    if (!checker.check()) m_constraint_consistent = false;
                }
            }
        }
        
        return isConstraintConsistent();
    }
    
    /**
     * Resets the constraint consistency.
     * 
     */
    private void resetConstraintConsistent()
    {
        assert (!isDeepImmutable());
        m_constraint_consistent = false;
    }
    
    /**
     * Sets the constraint consistency.
     * 
     * @param constraint_consistent
     *            whether the data tree is constraint consistent.
     */
    public void setConstraintConsistent(boolean constraint_consistent)
    {
        assert (!isDeepImmutable());
        m_constraint_consistent = constraint_consistent;
    }
    
    /**
     * Returns whether the data tree is deep immutable.
     * 
     * @return <code>true</code> if the data tree is deep immutable; <code>false</code> otherwise.
     */
    public boolean isDeepImmutable()
    {
        return m_deep_immutable;
    }
    
    /**
     * Sets the data tree to be deep immutable. This can only be set once, and cannot be unset thereafter.
     * 
     */
    public void setDeepImmutable()
    {
        // Flag can only be set once
        assert (!isDeepImmutable()) : "Data tree is already deep immutable";
        
        // Data tree must already be type consistent and constraint consistent
        assert (isTypeConsistent() && isConstraintConsistent()) : "Data tree must be type and constraint consistent";
        
        m_deep_immutable = true;
    }
    
    /**
     * Returns the schema tree.
     * 
     * @return the schema tree if there is one; <code>null</code> otherwise.
     */
    public SchemaTree getSchemaTree()
    {
        return getType();
    }
    
    /**
     * Notifies the data tree that part of the data tree has changed. Type consistency, name consistency and constraint consistency
     * are unset.
     * 
     */
    public void notifyChanged()
    {
        // Check that the data tree can be modified
        assert (!isDeepImmutable());
        
        // Unset type consistency and constraint consistency - checks have to be run subsequently
        resetTypeConsistent();
        resetConstraintConsistent();
    }
    
    @Override
    public String toString()
    {
        return "data tree:\n" + getRootValue().toString();
    }
}
