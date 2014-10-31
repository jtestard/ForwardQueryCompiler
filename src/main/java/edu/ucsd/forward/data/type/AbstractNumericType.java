/**
 * 
 */
package edu.ucsd.forward.data.type;

import edu.ucsd.forward.data.value.NumericValue;

/**
 * Abstract class for implementing numeric types.
 * 
 * @param <T>
 *            the corresponding value
 * @param <C>
 *            a class implementing the Comparable interface.
 * 
 * @author Michalis Petropoulos
 */
@SuppressWarnings("serial")
public abstract class AbstractNumericType<T extends NumericValue<C>, C extends Comparable<?>> extends AbstractPrimitiveType<T, C>
        implements NumericType
{
    
}
