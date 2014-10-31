/**
 * 
 */
package edu.ucsd.forward.query.logical.dml;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.AbstractUnaryOperator;
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.Project;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;

/**
 * The abstract class for DML operator.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public abstract class AbstractDmlOperator extends AbstractUnaryOperator implements DmlOperator
{
    @SuppressWarnings("unused")
    private static final Logger   log            = Logger.getLogger(AbstractDmlOperator.class);
    
    public static final String    COUNT_ATTR     = "count";
    
    /**
     * The target data source name.
     */
    private String                m_target_data_source;
    
    /**
     * The target.
     */
    private Term                  m_target_term;
    
    /**
     * The target tuple type.
     */
    private TupleType             m_target_tuple_type;
    
    /**
     * The primary keys of the target data object.
     */
    private Set<RelativeVariable> m_primary_keys = new HashSet<RelativeVariable>();
    
    protected AbstractDmlOperator()
    {
        
    }
    
    /**
     * Constructs with the alias.
     * 
     * @param target_data_source
     *            the target data source.
     * @param target_term
     *            the target term to modify.
     */
    protected AbstractDmlOperator(String target_data_source, Term target_term)
    {
        super();
        
        assert (target_data_source != null);
        m_target_data_source = target_data_source;
        
        assert (target_term != null);
        m_target_term = target_term;
    }
    
    @Override
    public String getTargetDataSourceName()
    {
        return m_target_data_source;
    }
    
    @Override
    public Term getTargetTerm()
    {
        return m_target_term;
    }
       
    @Override
    public TupleType getTargetTupleType()
    {
        return m_target_tuple_type;
    }
    
    /**
     * Sets the the target tuple type.
     * 
     * @param target_tuple_type
     *            the target tuple type.
     */
    protected void setTargetTupleType(TupleType target_tuple_type)
    {
        assert (target_tuple_type != null);
        
        m_target_tuple_type = target_tuple_type;
    }
    
    /**
     * @return the primary_keys
     */
    public Set<RelativeVariable> getPrimaryKeys()
    {
        return m_primary_keys;
    }
    
    /**
     * @param primaryKeys
     *            the primary_keys to set
     */
    public void setPrimaryKeys(Set<RelativeVariable> primaryKeys)
    {
        m_primary_keys = primaryKeys;
    }
    
    public void addPrimaryKey(RelativeVariable pk)
    {
        m_primary_keys.add(pk);
    }
    
    @Override
    public List<Parameter> getBoundParametersUsed()
    {
        return Collections.<Parameter> emptyList();
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        return Collections.<Parameter> emptyList();
    }
    
    @Override
    public void updateOutputInfo() throws QueryCompilationException
    {
        // Output info
        OutputInfo output_info = new OutputInfo();
        
        RelativeVariable new_var = new RelativeVariable(Project.PROJECT_ALIAS);
        TupleType tup_type = new TupleType();
        tup_type.setAttribute(COUNT_ATTR, new IntegerType());
        new_var.setType(tup_type);
        output_info.add(new_var, null);
        
        // Make sure the target data source is valid
        try
        {
            QueryProcessorFactory.getInstance().getDataSourceMetaData(m_target_data_source);
        }
        catch (DataSourceException e)
        {
            // This should never happen because the check is being done earlier.
            assert (false);
        }
        
        this.setOutputInfo(output_info);
    }
    
}
