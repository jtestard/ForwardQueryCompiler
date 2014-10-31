package edu.ucsd.forward.query.logical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeConverter;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.exception.ExceptionMessages.QueryCompilation;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

/**
 * Represents the offset and fetch logical operator.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class OffsetFetch extends AbstractUnaryOperator
{
    private Term m_offset;
    
    private Term m_fetch;
    
    /**
     * Initializes an instance of the operator.
     */
    public OffsetFetch()
    {
        m_offset = null;
        m_fetch = null;
    }
    
    /**
     * Gets the offset.
     * 
     * @return the offset.
     */
    public Term getOffset()
    {
        return m_offset;
    }
    
    /**
     * Sets the offset.
     * 
     * @param offset
     *            the offset.
     */
    public void setOffset(Term offset)
    {
        assert (offset != null);
        
        m_offset = offset;
    }
    
    /**
     * Gets the fetch.
     * 
     * @return the fetch.
     */
    public Term getFetch()
    {
        return m_fetch;
    }
    
    /**
     * Sets the fetch.
     * 
     * @param fetch
     *            the fetch.
     */
    public void setFetch(Term fetch)
    {
        assert (fetch != null);
        
        m_fetch = fetch;
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        List<Variable> result = new ArrayList<Variable>();
        
        if (m_offset != null)
        {
            result.addAll(m_offset.getVariablesUsed());
        }
        
        if (m_fetch != null)
        {
            result.addAll(m_fetch.getVariablesUsed());
        }
        
        return result;
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        List<Parameter> result = new ArrayList<Parameter>();
        
        if (m_offset != null)
        {
            result.addAll(m_offset.getFreeParametersUsed());
        }
        
        if (m_fetch != null)
        {
            result.addAll(m_fetch.getFreeParametersUsed());
        }
        
        return result;
    }
    
    @Override
    public List<Parameter> getBoundParametersUsed()
    {
        return Collections.emptyList();
    }
    
    @Override
    public void updateOutputInfo() throws QueryCompilationException
    {
        // Output info
        OutputInfo output_info = new OutputInfo();
        
        if (m_offset != null)
        {
            Type offset_type = m_offset.inferType(this.getChildren());
            if (!TypeConverter.getInstance().canImplicitlyConvert(offset_type, TypeEnum.LONG.get()))
            {
                throw new QueryCompilationException(QueryCompilation.INVALID_OFFSET_PROVIDED_TYPE, m_offset.getLocation());
            }
            
            // No need to throw exception since the top-most project operator's output object name hides all data object names.
            assert (m_offset.getBoundParametersUsed().isEmpty());
        }
        
        if (m_fetch != null)
        {
            Type fetch_type = m_fetch.inferType(this.getChildren());
            if (!TypeConverter.getInstance().canImplicitlyConvert(fetch_type, TypeEnum.LONG.get()))
            {
                throw new QueryCompilationException(QueryCompilation.INVALID_FETCH_PROVIDED_TYPE, m_fetch.getLocation());
            }
            
            // No need to throw exception since the top-most project operator's output object name hides all data object names.
            assert (m_fetch.getBoundParametersUsed().isEmpty());
        }
        
        // Copy the input variables and keys
        for (RelativeVariable var : this.getInputVariables())
        {
            output_info.add(var, var);
        }
        output_info.setKeyTerms(this.getChild().getOutputInfo().getKeyTerms());
        
        // Set the output_ordered flag of the output info
        output_info.setOutputOrdered(this.getChild().getOutputInfo().isOutputOrdered());
        
        // Set the actual output info
        this.setOutputInfo(output_info);
    }
    
    @Override
    public String toExplainString()
    {
        String str = super.toExplainString() + " ";
        
        if (m_offset != null)
        {
            str += "offset " + m_offset.toExplainString() + " ";
        }
        
        if (m_fetch != null)
        {
            str += "fetch " + m_fetch.toExplainString();
        }
        
        return str;
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitOffsetFetch(this);
    }
    
    @Override
    public Operator copy()
    {
        OffsetFetch copy = new OffsetFetch();
        super.copy(copy);
        
        copy.m_offset = m_offset;
        copy.m_fetch = m_fetch;
        
        return copy;
    }
    
}
