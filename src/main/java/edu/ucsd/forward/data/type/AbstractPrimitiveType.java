/**
 * 
 */
package edu.ucsd.forward.data.type;

import edu.ucsd.forward.data.value.PrimitiveValue;

/**
 * Abstract class for implementing primitive types.
 * 
 * @param <T>
 *            the corresponding value
 * @param <C>
 *            a class implementing the Comparable interface.
 * 
 * @author Kian Win
 * @author Yupeng
 * @author Michalis Petropoulos
 */
@SuppressWarnings("serial")
public abstract class AbstractPrimitiveType<T extends PrimitiveValue<C>, C extends Comparable<?>> extends AbstractScalarType<T>
        implements PrimitiveType
{
    
}
