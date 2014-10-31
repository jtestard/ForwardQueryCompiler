/**
 * 
 */
package edu.ucsd.forward.query.logical.term;

import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.data.value.XhtmlValue;
import edu.ucsd.forward.query.ast.literal.StringLiteral;
import edu.ucsd.forward.query.logical.Operator;

/**
 * Represents a constant scalar value or a null value.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class Constant extends AbstractTerm
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(Constant.class);
    
    private Value               m_value;
    
    /**
     * Constructs the constant scalar value or the null value.
     * 
     * @param value
     *            the value.
     */
    public Constant(Value value)
    {
        assert (value != null);
        m_value = value;
        
        this.setType(TypeEnum.get(m_value.getTypeClass()));
    }
    
    /**
     * Default constructor.
     */
    @SuppressWarnings("unused")
    private Constant()
    {
        
    }
    
    /**
     * Returns the constant value.
     * 
     * @return the value.
     */
    public Value getValue()
    {
        return m_value;
    }
    
    @Override
    public Type inferType(List<Operator> operators)
    {
        return this.getType();
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        return Collections.emptyList();
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        return Collections.<Parameter> emptyList();
    }
    
    @Override
    public List<Parameter> getBoundParametersUsed()
    {
        return Collections.emptyList();
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        return true;
    }
    
    @Override
    public String getDefaultProjectAlias()
    {
        return "constant";
    }
    
    @Override
    public String toExplainString()
    {
        if (m_value instanceof StringValue || m_value instanceof XhtmlValue)
        {
            return StringLiteral.QUOTE + m_value.toString() + StringLiteral.QUOTE;
        }
        
        return m_value.toString();
    }
    
    @Override
    public String toString()
    {
        return this.toExplainString();
    }
    
    @Override
    public Term copy()
    {
        Constant copy = new Constant(ValueUtil.cloneNoParentNoType(m_value));
        super.copy(copy);
        
        return copy;
    }
    
    @Override
    public Term copyWithoutType()
    {
        return copy();
    }
    
}
