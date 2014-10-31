/**
 * 
 */
package edu.ucsd.forward.data.type;

import java.util.Collection;
import java.util.Collections;

import edu.ucsd.forward.data.constraint.ConstraintUtil;
import edu.ucsd.forward.data.source.DataSource;

/**
 * A JSON type for JSON value.
 * 
 * @author Yupeng
 * 
 */
@SuppressWarnings("serial")
public class JsonType extends AbstractType<AbstractComplexType> implements NoSqlType
{
    
    public JsonType()
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
        // display non-null constraint
        if (ConstraintUtil.getNonNullConstraint(this) != null)
        {
            sb.append(" NOT NULL ");
        }
    }
}
