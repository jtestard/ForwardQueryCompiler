/**
 * 
 */
package edu.ucsd.forward.data.value;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.util.tree.TreeNode;

/**
 * A value.
 * 
 * @author Kian Win
 * @author Yupeng
 * 
 */
public interface Value extends TreeNode<Value>,Serializable
{
    /**
     * Returns the corresponding type class.
     * 
     * @return the corresponding type class.
     */
    public Class<? extends Type> getTypeClass();
    
    /**
     * Returns the type.
     * 
     * @return the type.
     */
    @Deprecated
    public Type getType();
    
    /**
     * Sets the type.
     * 
     * @param type
     *            - the type.
     */
    @Deprecated
    public void setType(Type type);
    
    /**
     * Returns the dataset.
     * 
     * @return the dataset if the value is contained in one, or is a dataset itself; <code>null</code> otherwise.
     */
    public DataTree getDataTree();
    
    /**
     * Get all ancestors and self of type Value.
     * 
     * @return the list with the ancestors and self of type Value.
     */
    public List<Value> getValueAncestorsAndSelf();
    
    /**
     * Get all descendants of type Value.
     * 
     * @return the list with the descendants of type Value.
     */
    public Collection<Value> getValueDescendants();
    
    /**
     * Get all descendants of type ScalarValue.
     * 
     * @return the list with the descendants of type ScalarValue.
     */
    public Collection<ScalarValue> getScalarValueDescendants();
    
    /**
     * Returns the lowest collection tuple ancestor (including itself).
     * 
     * @return the lowest collection tuple ancestor, or <code>null</code> if there is no such.
     */
    public TupleValue getLowestCollectionTupleAncestor();
    
}
