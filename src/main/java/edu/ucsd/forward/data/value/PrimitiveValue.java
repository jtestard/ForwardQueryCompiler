/**
 * 
 */
package edu.ucsd.forward.data.value;

import edu.ucsd.app2you.util.identity.Immutable;
import edu.ucsd.forward.data.type.PrimitiveType;

/**
 * A primitive value is a scalar value that is represented by a Java object that can be compared, in addition to be deep equaled.
 * 
 * @author Kian Win
 * @author Michalis Petropoulos
 * 
 * @param <C>
 *            a class implementing the Comparable interface.
 */
public interface PrimitiveValue<C extends Comparable<?>> extends ScalarValue, Immutable
{
    @Override
    @Deprecated
    public PrimitiveType getType();
    
    @Override
    public C getObject();
}
