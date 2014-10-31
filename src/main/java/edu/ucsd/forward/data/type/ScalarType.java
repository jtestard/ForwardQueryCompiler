/**
 * 
 */
package edu.ucsd.forward.data.type;

/**
 * A scalar type.
 * 
 * @author Kian Win
 * @author Michalis Petropoulos
 * 
 */
public interface ScalarType extends Type
{
    @Override
    public Type getParent();
    
}
