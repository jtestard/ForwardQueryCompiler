/**
 * 
 */
package edu.ucsd.forward.data.type.extended;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.type.AbstractScalarType;
import edu.ucsd.forward.data.value.ScalarValue;

/**
 * An ANY_SCALAR extended type.
 * 
 * @author Kian Win
 * 
 */
public class AnyScalarType extends AbstractScalarType<ScalarValue>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AnyScalarType.class);
    
    @Override
    public String toString()
    {
        return "any_scalar_type";
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    protected String getTypeName()
    {
        throw new UnsupportedOperationException();
    }
    
}
