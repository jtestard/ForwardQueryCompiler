/**
 * 
 */
package edu.ucsd.forward.data.value;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.Type;

/**
 * An abstract class for values that can contain other values.
 * 
 * @author Romain Vernoux
 * 
 * @param <T>
 *            - the corresponding type.
 */
@SuppressWarnings("serial")
public abstract class AbstractComplexValue<T extends Type> extends AbstractValue<T, AbstractComplexValue<?>>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractComplexValue.class);
}
