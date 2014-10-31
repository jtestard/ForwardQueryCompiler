/**
 * 
 */
package edu.ucsd.forward.data.value;

import edu.ucsd.forward.data.type.PrimitiveType;

/**
 * Abstract class for implementing primitive values.
 * 
 * @author Kian Win
 * @author Michalis Petropoulos
 * 
 * @param <T>
 *            the corresponding type.
 * @param <C>
 *            a class implementing the Comparable interface.
 */
@SuppressWarnings("serial")
public abstract class AbstractPrimitiveValue<T extends PrimitiveType, C extends Comparable<?>> extends AbstractScalarValue<T>
        implements PrimitiveValue<C>
{
    
    @Override
    public String toString()
    {
        return getObject().toString();
    }
    
}
