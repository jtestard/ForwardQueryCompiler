/**
 * 
 */
package edu.ucsd.forward.data.value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.util.tree.AbstractTreeNode;

/**
 * Abstract class for implementing values.
 * 
 * @author Kian Win
 * 
 * @param <T>
 *            the corresponding type.
 * @param <P>
 *            the parent node.
 */
@SuppressWarnings("serial")
public abstract class AbstractValue<T extends Type, P extends Value> extends AbstractTreeNode<Value, Value, P> implements Value
{
    
    private T m_type;
    
    @Override
    @Deprecated
    public T getType()
    {
        return m_type;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void setType(Type type)
    {
        assert (type != null);
        
        // The data instance needs to be compatible with the type. Exceptions to the rule includes the null value.
        assert ((type instanceof SchemaTree && this instanceof DataTree) || type.getClass().equals(this.getTypeClass()) || this instanceof NullValue) : "Data and type are not compatible. Type: "
                + type.getClass() + ", Data: " + this.getTypeClass();
        
        // Nothing to do
        if (m_type == type) return;
        
        DataTree data_tree = getDataTree();
        if (data_tree != null)
        {
            data_tree.notifyChanged();
        }
        
        m_type = (T) type;
    }
    
    @Override
    public abstract Class<? extends T> getTypeClass();
    
    @Override
    public DataTree getDataTree()
    {
        Value value = getRoot();
        if (value instanceof DataTree)
        {
            return (DataTree) value;
        }
        else
        {
            return null;
        }
    }
    
    @Override
    protected void setParent(P parent)
    {
        // Re-parenting a node requires notifying both the old root and the new root
        
        Value old_parent = getParent();
        Value new_parent = parent;
        
        if (old_parent == new_parent) return;
        
        if (old_parent != null && old_parent.getDataTree() != null)
        {
            old_parent.getDataTree().notifyChanged();
        }
        
        if (new_parent != null && new_parent.getDataTree() != null)
        {
            new_parent.getDataTree().notifyChanged();
        }
        
        super.setParent(parent);
    }
    
    /**
     * Get all descendants and self of type Value.
     * 
     * @return the list with the descendants and self of type Value.
     */
    public List<Value> getValueAncestorsAndSelf()
    {
        List<Value> list = new ArrayList<Value>();
        for (Value node : getAncestorsAndSelf())
        {
            if (node instanceof Value) list.add((Value) node);
        }
        return list;
    }
    
    /**
     * Get all descendants of type Value.
     * 
     * @return the list with the descendants of type Value.
     */
    public Collection<Value> getValueDescendants()
    {
        List<Value> list = new ArrayList<Value>();
        for (Value child : getChildren())
        {
            if (child instanceof Value) list.add((Value) child);
            list.addAll(child.getValueDescendants());
        }
        return list;
    }
    
    /**
     * Get all descendants of type ScalarValue.
     * 
     * @return the list with the descendants of type ScalarValue.
     */
    public Collection<ScalarValue> getScalarValueDescendants()
    {
        List<ScalarValue> list = new ArrayList<ScalarValue>();
        for (Value child : getChildren())
        {
            if (child instanceof ScalarValue) list.add((ScalarValue) child);
            list.addAll(child.getScalarValueDescendants());
        }
        return list;
    }
    
    @Override
    public TupleValue getLowestCollectionTupleAncestor()
    {
        Value c = this;
        while (c != null && !(c.getParent() instanceof CollectionValue))
        {
            c = c.getParent();
        }
        
        return (TupleValue) c;
    }
}
