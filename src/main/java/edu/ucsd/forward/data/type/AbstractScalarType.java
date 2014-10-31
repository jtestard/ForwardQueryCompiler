/**
 * 
 */
package edu.ucsd.forward.data.type;

import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.data.constraint.ConstraintUtil;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.ast.literal.StringLiteral;

/**
 * Abstract class for implementing scalar types.
 * 
 * @param <T>
 *            the corresponding value
 * @author Kian Win
 * @author Yupeng
 * @author Michalis Petropoulos
 */
@SuppressWarnings("serial")
public abstract class AbstractScalarType<T extends ScalarValue> extends AbstractType<AbstractComplexType> implements ScalarType
{
    
    /**
     * Gets the name of the type.
     * 
     * @return the name of the type.
     */
    protected abstract String getTypeName();
    
    /**
     * Default constructor.
     */
    protected AbstractScalarType()
    {
        
    }
    
    @Override
    public List<? extends Type> getChildren()
    {
        return Collections.emptyList();
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(getTypeName());
        
        // display non-null constraint
        if (ConstraintUtil.getNonNullConstraint(this) != null)
        {
            sb.append(" NOT NULL ");
        }
        
        // default value constraints
        if (ConstraintUtil.getAutoIncrementConstraint(this) != null)
        {
            sb.append(" AUTO INCREMENT ");
        }
        else
        {
            Value default_value = getDefaultValue();
            String default_str = (default_value instanceof StringValue)
                    ? StringLiteral.escape((StringValue) default_value)
                    : default_value.toString();
            
            sb.append(" DEFAULT " + default_str);
            
        }
    }
    
    @Override
    public String toString()
    {
        String s = TypeEnum.getName(this);
        
        return s;
    }
}
