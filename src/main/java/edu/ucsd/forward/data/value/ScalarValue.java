/**
 * 
 */
package edu.ucsd.forward.data.value;

import edu.ucsd.forward.data.type.ScalarType;

/**
 * A scalar value is represented by a Java object that can be deep equaled.
 * 
 * @author Kian Win
 * 
 */
public interface ScalarValue extends Value
{
    /**
     * 
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.value.Value#getType()
     */
    @Override
    @Deprecated
    public ScalarType getType();
    
    /**
     * 
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.util.tree.TreeNode#getParent()
     */
    @Override
    public Value getParent();
    
    /**
     * Returns the corresponding type class.
     * 
     * @return the corresponding type class.
     */
    @Override
    public Class<? extends ScalarType> getTypeClass();
    
    /**
     * Returns a corresponding Java object.
     * 
     * @return a corresponding Java object.
     */
    public Object getObject();
    
    /**
     * Determines whether the corresponding Java object of this scalar value is equal to that of the provided scalar value.
     * 
     * This method is necessary, as the corresponding Java object may require calling methods other than equals() to determine
     * equality. One example with such requirement is a DOM Node, which requires calling isEqualNode().
     * 
     * @param scalar_value
     *            the other scalar value.
     * @return <code>true</code> if the two corresponding Java objects are equal; <code>false</code> otherwise.
     */
    public boolean deepEquals(ScalarValue scalar_value);
}
