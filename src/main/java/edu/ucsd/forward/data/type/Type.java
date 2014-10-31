/**
 * 
 */
package edu.ucsd.forward.data.type;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import edu.ucsd.forward.data.constraint.Constraint;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.DataSourceCompliance;
import edu.ucsd.forward.query.explain.QueryExpressionPrinter;
import edu.ucsd.forward.util.tree.TreeNode;

/**
 * A type.
 * 
 * @author Kian Win
 * @author Yupeng
 * 
 */
public interface Type extends TreeNode<Type>, DataSourceCompliance, QueryExpressionPrinter,Serializable
{
    /**
     * Returns a schema tree.
     * 
     * @return a schema tree if the type is contained in one, or is a schema tree itself; <code>null</code> otherwise.
     */
    public SchemaTree getSchemaTree();
    
    /**
     * Gets the default value of the type.
     * 
     * @return the default value of the type. There is always a default value, hence <code>null</code> is never returned.
     */
    public Value getDefaultValue();
    
    /**
     * Sets the default value.
     * 
     * @param default_value
     *            the default value.
     */
    public void setDefaultValue(Value default_value);
    
    /**
     * Returns the constraints of this type.
     * 
     * @return the constraints of this type.
     */
    public List<Constraint> getConstraints();
    
    /**
     * Adds a constraint to the type.
     * 
     * @param constraint
     *            - the constraint to add.
     */
    public void addConstraint(Constraint constraint);
    
    /**
     * Removes a constraint from the type.
     * 
     * @param constraint
     *            - the constraint to remove.
     * @return whether the constraint is found.
     */
    public boolean removeConstraint(Constraint constraint);
    
    /**
     * Get all descendants and self of type Type.
     * 
     * @return the list with the descendants and self of type Type.
     */
    public List<Type> getTypeAncestorsAndSelf();
    
    /**
     * Get all descendants of type Type.
     * 
     * @return the list with the descendants of type Type.
     */
    public Collection<Type> getTypeDescendants();
    
}
