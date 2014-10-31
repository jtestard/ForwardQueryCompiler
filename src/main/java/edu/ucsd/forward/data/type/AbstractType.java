/**
 * 
 */
package edu.ucsd.forward.data.type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.constraint.Constraint;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.util.tree.AbstractTreeNode;

/**
 * Abstract class for implementing types.
 * 
 * @author Kian Win
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 * @param <P>
 *            the parent node.
 * 
 */
@SuppressWarnings("serial")
public abstract class AbstractType<P extends Type> extends AbstractTreeNode<Type, SchemaTree, P> implements Type
{
    
    private Value            m_default_value = null;
    
    private List<Constraint> m_constraints;
    
    /**
     * Constructor.
     */
    public AbstractType()
    {
        m_constraints = new ArrayList<Constraint>();
    }
    
    @Override
    public Value getDefaultValue()
    {
        if (this.m_default_value == null)
        {
            // Initialize the value according to the specific value type.
            if (this instanceof SchemaTree)
            {
                m_default_value = new NullValue(SchemaTree.class);
                m_default_value.setType(this);
            }
            else if (this instanceof CollectionType)
            {
                m_default_value = new CollectionValue();
                m_default_value.setType(this);
            }
            else if (this instanceof TupleType)
            {
                m_default_value = new NullValue(TupleType.class);
                m_default_value.setType(this);
            }
            else if (this instanceof SwitchType)
            {
                m_default_value = new NullValue(SwitchType.class);
                m_default_value.setType(this);
            }
            else if (this instanceof ScalarType)
            {
                m_default_value = new NullValue(this.getClass());
                m_default_value.setType(this);
            }
            else if (this instanceof NullType)
            {
                m_default_value = new NullValue(NullType.class);
                m_default_value.setType(this);
            }
            else if (this instanceof JsonType)
            {
                m_default_value = new NullValue(JsonType.class);
                m_default_value.setType(this);
            }
            else if (this instanceof UnknownType)
            {
                m_default_value = new NullValue(UnknownType.class);
                m_default_value.setType(this);
            }
            else
            {
                throw new UnsupportedOperationException();
            }
        }
        
        return m_default_value;
    }
    
    @Override
    public void setDefaultValue(Value default_value)
    {
        // FIXME Type check the default value
        assert (default_value != null);
        
        Value default_value_copy = ValueUtil.cloneNoParentNoType(default_value);
        m_default_value = default_value_copy;
    }
    
    @Override
    public List<Constraint> getConstraints()
    {
        return Collections.unmodifiableList(m_constraints);
    }
    
    @Override
    public void addConstraint(Constraint constraint)
    {
        m_constraints.add(constraint);
    }
    
    @Override
    public boolean removeConstraint(Constraint constraint)
    {
        boolean success = m_constraints.remove(constraint);
        
        return success;
    }
    
    @Override
    public SchemaTree getSchemaTree()
    {
        Type type = getRoot();
        if (type instanceof SchemaTree)
        {
            return (SchemaTree) type;
        }
        else
        {
            return null;
        }
    }
    
    @Override
    public List<Type> getTypeAncestorsAndSelf()
    {
        List<Type> list = new ArrayList<Type>();
        for (Type node : getAncestorsAndSelf())
        {
            if (node instanceof Type) list.add((Type) node);
        }
        return list;
    }
    
    @Override
    public Collection<Type> getTypeDescendants()
    {
        List<Type> list = new ArrayList<Type>();
        for (Type child : getChildren())
        {
            if (child instanceof Type) list.add((Type) child);
            list.addAll(child.getTypeDescendants());
        }
        return list;
    }
    
    @Override
    protected void setParent(P parent)
    {
        super.setParent(parent);
    }
    
    /**
     * Gets the whitespace as indentation to format the SQL++ representation.
     * 
     * @return the indent whitespace.
     */
    protected String getIndent()
    {
        // The number of indent is the number of the type's ancestors excluding the schema tree and collections' tuple type.
        int number = this.getAncestors().size() - 1 - this.getAncestors(CollectionType.class).size();
        String indent = "";
        for (int i = 0; i < number; i++)
        {
            indent += "    ";
        }
        return indent;
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        switch (metadata.getDataModel())
        {
            case SQLPLUSPLUS:
                // Always compliant with SQL++ data sources
                return true;
            case RELATIONAL:
                // Check whether the type is relational, that is, the type is a scalar type.
                if (!(this instanceof ScalarType) && !(this instanceof NullType) && !(this instanceof TupleType))
                {
                    return false;
                }
                
                return true;
            default:
                throw new AssertionError();
        }
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        this.toQueryString(sb, 0, null);
        return sb.toString();
    }
    
}
