/**
 * 
 */
package edu.ucsd.forward.data.type;

import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.data.source.DataSource;

/**
 * A type used primarily as the return type of void functions.
 * 
 * @author Michalis Petropoulos
 */
@SuppressWarnings("serial")
public class NullType extends AbstractType<AbstractComplexType>
{
    
    public static final String UNKNOWN = "unknown";
    
    /**
     * Constructs the null type.
     */
    public NullType()
    {
    }
    
    @Override
    public List<TupleType> getChildren()
    {
        return Collections.emptyList();
    }
    
    @Override
    public String toString()
    {
        return TypeEnum.NULL.getName();
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(TypeEnum.NULL.getName());
    }
    
}
