package edu.ucsd.forward.query.logical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.source.SchemaObjectHandle;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

/**
 * Represents an operator that copies the query result of a nested query plan to a target data source.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class Copy extends AbstractOperator implements NestedPlansOperator
{
    /**
     * The handle of the target schema object of the operator.
     */
    private SchemaObjectHandle m_handle;
    
    /**
     * The logical plan to copy.
     */
    private LogicalPlan        m_copy_plan;
    
    /**
     * Creates an instance of the operator.
     * 
     * @param data_src_name
     *            the target data source name.
     * @param schema_obj_name
     *            the target schema object name of the operator.
     * @param copy
     *            the logical plan to copy.
     */
    public Copy(String data_src_name, String schema_obj_name, LogicalPlan copy)
    {
        m_handle = new SchemaObjectHandle(data_src_name, schema_obj_name);
        
        assert (copy != null);
        m_copy_plan = copy;
    }
    
    /**
     * Private constructor.
     */
    @SuppressWarnings("unused")
    private Copy()
    {
        
    }
    
    /**
     * Returns the name of the target data source.
     * 
     * @return the name of the target data source.
     */
    public String getTargetDataSourceName()
    {
        return m_handle.getDataSourceName();
    }
    
    /**
     * Returns the name of the target schema object.
     * 
     * @return the name of the target schema object.
     */
    public String getTargetSchemaObjectName()
    {
        return m_handle.getSchemaObjectName();
    }
    
    /**
     * Gets the logical plan to copy.
     * 
     * @return the logical plan to copy.
     */
    public LogicalPlan getCopyPlan()
    {
        return m_copy_plan;
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        return Collections.<Variable> emptyList();
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        // From the parameters of the copy logical plan, keep only the parameters that do not appear in the input info.
        List<Parameter> free_params = new ArrayList<Parameter>();
        for (Parameter free_param : m_copy_plan.getFreeParametersUsed())
        {
            if (!this.getInputVariables().contains(free_param.getVariable()))
            {
                free_params.add(free_param);
            }
        }
        
        return free_params;
    }
    
    @Override
    public List<Parameter> getBoundParametersUsed()
    {
        return Collections.<Parameter> emptyList();
    }
    
    @Override
    public List<LogicalPlan> getLogicalPlansUsed()
    {
        return Collections.<LogicalPlan> singletonList(m_copy_plan);
    }
    
    @Override
    public void updateOutputInfo() throws QueryCompilationException
    {
        // No need to update the output info of the nested logical plan
        
        // Set the cardinality estimate based on the one of the root operator of the nested plan
        this.setCardinalityEstimate(m_copy_plan.getRootOperator().getCardinalityEstimate());
        
        // Output info
        OutputInfo output_info = new OutputInfo();
        
        // Add one new variable
        RelativeVariable new_var = new RelativeVariable(m_handle.getSchemaObjectName());
        // Use the output type of the nested plan
        new_var.setType(m_copy_plan.getOutputType());
        // FIXME The provenance of the new variables should be the nested plan, not null
        output_info.add(new_var, null);
        
        // Set the actual output info
        this.setOutputInfo(output_info);
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        // FIXME Check that the storage system of the execution data source is INMEMORY, the only storage system that can execute
        // the operator implementation for now.
        switch (metadata.getStorageSystem())
        {
            case INMEMORY:
                return true;
            default:
                return false;
        }
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitCopy(this);
    }
    
    @Override
    public Operator copy()
    {
        Copy copy = new Copy(m_handle.getDataSourceName(), m_handle.getSchemaObjectName(), m_copy_plan.copy());
        super.copy(copy);
        
        return copy;
    }
    
}
