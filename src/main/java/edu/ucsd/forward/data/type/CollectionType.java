/**
 * 
 */
package edu.ucsd.forward.data.type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsd.app2you.util.StringUtil;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.constraint.Constraint;
import edu.ucsd.forward.data.constraint.LocalPrimaryKeyConstraint;
import edu.ucsd.forward.data.constraint.SingletonConstraint;
import edu.ucsd.forward.data.index.IndexDeclaration;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceException;

/**
 * A collection type.
 * 
 * @author Kian Win
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class CollectionType extends AbstractComplexType
{
    
    private Type                          m_children_type;
    
    private Map<String, IndexDeclaration> m_index_declarations;
    
    private boolean                       m_ordered = false;
    
    /**
     * Constructs the collection type.
     */
    public CollectionType()
    {
        // FIXME change to Any type
        this(new TupleType());
    }
    
    /**
     * Constructs the collection type with a given tuple type.
     * 
     * @param tuple_type
     *            the tuple type.
     */
    @Deprecated
    public CollectionType(TupleType tuple_type)
    {
        assert (tuple_type != null);
        
        setTupleType(tuple_type);
        m_index_declarations = new HashMap<String, IndexDeclaration>();
    }
    
    /**
     * Constructs the collection type with a given type.
     * 
     * @param children_type
     *            the type.
     */
    public CollectionType(Type children_type)
    {
        assert (children_type != null);
        
        setChildrenType(children_type);
        m_index_declarations = new HashMap<String, IndexDeclaration>();
    }
    
    @Override
    public List<Type> getChildren()
    {
        return Arrays.asList(m_children_type);
    }
    
    /**
     * Tells if this collection type is a list or a bag.
     * 
     * @return <code>true</code> if this collection type is a list, <code>false</code> otherwise.
     */
    public boolean isOrdered()
    {
        return m_ordered;
    }
    
    /**
     * Sets the flag telling if this collection type is a list or a bag.
     * 
     * @param flag
     *            should be <code>true</code> if this collection type is a list, <code>false</code> otherwise.
     */
    public void setOrdered(boolean flag)
    {
        m_ordered = flag;
    }
    
    /**
     * Adds an index to the collection.
     * 
     * @param declaration
     *            the index declaration.
     * @throws DataSourceException
     *             if the index with same name exists
     */
    public void addIndex(IndexDeclaration declaration) throws DataSourceException
    {
        assert declaration != null;
        String name = declaration.getName();
        if (m_index_declarations.containsKey(name))
        {
            throw new DataSourceException(edu.ucsd.forward.exception.ExceptionMessages.DataSource.INVALID_NEW_INDEX_NAME, name,
                                          this.toString());
        }
        m_index_declarations.put(name, declaration);
    }
    
    /**
     * Removes an existing index.
     * 
     * @param name
     *            the name of the index to remove.
     * @throws DataSourceException
     *             if the index with the name does not exist.
     */
    public void removeIndex(String name) throws DataSourceException
    {
        if (!m_index_declarations.containsKey(name))
        {
            throw new DataSourceException(edu.ucsd.forward.exception.ExceptionMessages.DataSource.UNKNOWN_INDEX_NAME,
                                          this.toString(), name);
        }
        m_index_declarations.remove(name);
    }
    
    /**
     * Gets the index declarations.
     * 
     * @return the index declarations.
     */
    public List<IndexDeclaration> getIndexDeclarations()
    {
        return new ArrayList<IndexDeclaration>(m_index_declarations.values());
    }
    
    /**
     * Returns the tuple type.
     * 
     * @return the tuple type.
     */
    @Deprecated
    public TupleType getTupleType()
    {
        assert (m_children_type instanceof TupleType);
        return (TupleType) m_children_type;
    }
    
    /**
     * Returns the children type.
     * 
     * @return the children type.
     */
    public Type getChildrenType()
    {
        return m_children_type;
    }
    
    /**
     * Sets the tuple type.
     * 
     * @param tuple_type
     *            - the tuple type.
     */
    @Deprecated
    public void setTupleType(TupleType tuple_type)
    {
        assert tuple_type != null : "The tuple type to be set cannot be null.";
        
        TupleType children_type = (TupleType) m_children_type;
        
        // Detach the old tuple type
        if (children_type != null) children_type.setParent(null);
        
        m_children_type = tuple_type;
        tuple_type.setParent(this);
    }
    
    /**
     * Sets the children type.
     * 
     * @param children_type
     *            - the children type.
     */
    @SuppressWarnings("unchecked")
    public void setChildrenType(Type children_type)
    {
        assert children_type != null : "The children type to be set cannot be null.";
        
        // Detach the old tuple type
        if (m_children_type != null) ((AbstractType<?>) children_type).setParent(null);
        
        m_children_type = children_type;
        /*
         * setParent() is defined on the abstract class so that it can be a protected method. However, casting the value to the
         * genericized abstract class causes a generics warning because of type erasure.
         */
        ((AbstractType<CollectionType>) m_children_type).setParent(this);
    }
    
    /**
     * Removes the tuple type.
     * 
     * @return the removed tuple type
     */
    @Deprecated
    public TupleType removeTupleType()
    {
        assert (m_children_type instanceof TupleType);
        TupleType tuple_type = (TupleType) m_children_type;
        
        if (tuple_type != null) tuple_type.setParent(null);
        m_children_type = null;
        return tuple_type;
    }
    
    /**
     * Removes the children type.
     * 
     * @return the removed children type
     */
    public Type removeChildrenType()
    {
        if (m_children_type != null) ((AbstractType<?>) m_children_type).setParent(null);
        Type type = m_children_type;
        m_children_type = null;
        return type;
    }
    
    @Override
    public void addConstraint(Constraint constraint)
    {
        // Accommodate a single local primary key constraint for now
        if (constraint instanceof LocalPrimaryKeyConstraint && this.hasLocalPrimaryKeyConstraint())
        {
            this.removeConstraint(this.getLocalPrimaryKeyConstraint());
        }
        super.addConstraint(constraint);
    }
    
    /**
     * Determines whether the collection type has a local primary key constraint.
     * 
     * @return true, if the collection type has a local primary key constraint; otherwise, false.
     */
    public boolean hasLocalPrimaryKeyConstraint()
    {
        for (Constraint constraint : this.getConstraints())
            if (constraint instanceof LocalPrimaryKeyConstraint) return true;
        
        return false;
    }
    
    /**
     * Returns the local primary key constraint, if any.
     * 
     * @return the local primary key constraint, if any; otherwise, null.
     */
    public LocalPrimaryKeyConstraint getLocalPrimaryKeyConstraint()
    {
        List<LocalPrimaryKeyConstraint> out = new ArrayList<LocalPrimaryKeyConstraint>();
        for (Constraint constraint : this.getConstraints())
            if (constraint instanceof LocalPrimaryKeyConstraint)
            {
                out.add((LocalPrimaryKeyConstraint) constraint);
            }
        
        assert (out.size() <= 1);
        
        return (out.size() == 1) ? out.get(0) : null;
    }
    
    /**
     * Check constraints are consistent with respect to the given schema tree. Only applies to the local constraints. I.e.,
     * excluding constraints of descendants.
     * 
     * @param schema_tree
     *            the schema tree
     * @return whether constraints are consistent.
     */
    public boolean checkConstraintsTypeConsistent(SchemaTree schema_tree)
    {
        for (Constraint c : getConstraints())
        {
            if (!c.isTypeConsistent(schema_tree)) return false;
        }
        
        return true;
    }
    
    @Override
    public String toString()
    {
        String indent_less = StringUtil.leftPad("", getHeight() - 2);
        String indent = StringUtil.leftPad("", getHeight() - 1);
        String indent_more = StringUtil.leftPad("", getHeight());
        
        /*-
         * Example:
         *  [ 
         *   tuple_type
         *  ]
         */
        
        StringBuilder sb = new StringBuilder();
        
        String open;
        String close;
        if(m_ordered)
        {
            open = "[";
            close = "]";
        }
        else
        {
            open = "{{";
            close = "}}";
        }
        
        sb.append("\n");
        sb.append(indent);
        sb.append(open + "\n");
        
        sb.append(indent_more);
        sb.append(m_children_type);
        sb.append("\n");
        
        sb.append(indent);
        sb.append(close + "\n");
        sb.append(indent_less);
        
        return sb.toString();
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        // FIXME: Adds list type, when we have order constraints working.
        sb.append(TypeEnum.COLLECTION.getName() + "(\n");
        TupleType tuple_type = getTupleType();
        // Display the tuple type
        boolean first = true;
        for (String attribute_name : tuple_type.getAttributeNames())
        {
            if (first)
            {
                first = false;
            }
            else
            {
                sb.append(",\n");
            }
            Type attribute = tuple_type.getAttribute(attribute_name);
            sb.append(((AbstractType<?>) attribute).getIndent() + attribute_name + " ");
            attribute.toQueryString(sb, tabs, data_source);
        }
        
        // Display the constraints
        sb.append(",\n");
        if (this.hasLocalPrimaryKeyConstraint())
        {
            sb.append(getIndent() + "PRIMARY KEY(");
            first = true;
            for (ScalarType type : this.getLocalPrimaryKeyConstraint().getAttributes())
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    sb.append(",");
                }
                
                sb.append(new SchemaPath(this.getTupleType(), type).getString().replace(SchemaPath.PATH_SEPARATOR, "."));
            }
            sb.append(")\n");
        }
        else
        {
            sb.append(getIndent());
            sb.append("NO PRIMARY KEY!\n");
        }
        sb.append(getIndent() + ")");
    }
    
    /**
     * Returns whether this collection type has a singleton constraint.
     * 
     * @return whether this collection type has a singleton constraint.
     */
    public boolean hasSingletonConstraint()
    {
        List<SingletonConstraint> out = new ArrayList<SingletonConstraint>();
        for (Constraint constraint : this.getConstraints())
            if (constraint instanceof SingletonConstraint)
            {
                out.add((SingletonConstraint) constraint);
            }
        
        assert (out.size() <= 1);
        
        return (out.size() == 1);
    }
    
}
