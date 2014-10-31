/**
 * 
 */
package edu.ucsd.forward.data.value;

import edu.ucsd.forward.data.type.NumericType;

/**
 * Abstract class for implementing numeric values.
 * 
 * @author Michalis Petropoulos
 * 
 * @param <T>
 *            the corresponding type.
 * @param <C>
 *            a class implementing the Comparable interface.
 */
@SuppressWarnings("serial")
public abstract class AbstractNumericValue<T extends NumericType, C extends Comparable<?>> extends AbstractPrimitiveValue<T, C>
        implements NumericValue<C>
{
    
}
