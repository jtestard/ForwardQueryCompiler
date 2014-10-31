/**
 * 
 */
package edu.ucsd.forward.data.value;

import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.data.type.ScalarType;

/**
 * Abstract class for implementing scalar values.
 * 
 * @author Kian Win
 * 
 * @param <T>
 *            - the corresponding type.
 */
@SuppressWarnings("serial")
public abstract class AbstractScalarValue<T extends ScalarType> extends AbstractValue<T, AbstractComplexValue<?>> implements ScalarValue
{
    
    @Override
    public List<? extends Value> getChildren()
    {
        return Collections.<Value> emptyList();
    }
    
    @Override
    public boolean deepEquals(ScalarValue scalar_value)
    {
        return getObject().equals(scalar_value.getObject());
    }
    
}
