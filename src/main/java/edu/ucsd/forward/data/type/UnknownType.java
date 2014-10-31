/**
 * 
 */
package edu.ucsd.forward.data.type;

import java.util.Collection;
import java.util.Collections;

import edu.ucsd.forward.data.source.DataSource;

/**
 * An unknown type.
 * 
 * @author Yupeng
 * 
 */
@SuppressWarnings("serial")
public class UnknownType extends AbstractType<AbstractComplexType> implements NoSqlType
{
    
    public UnknownType()
    {
    }
    
    @Override
    public Collection<? extends Type> getChildren()
    {
        return Collections.emptyList();
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(TypeEnum.getName(this));
    }
}
