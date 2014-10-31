package edu.ucsd.forward.query.logical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

/**
 * Represents an operator that contains a logical plan to be sent to a target data source for execution. The nested logical plan
 * does not have nested send plan operators.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class SendPlan extends AbstractOperator implements NestedPlansOperator
{
    /**
     * The logical plan to send.
     */
    private LogicalPlan     m_send_plan;
    
    /**
     * The bound parameters.
     */
    private List<Parameter> m_bound_params;
    
    /**
     * Creates an instance of the operator.
     * 
     * @param data_src_name
     *            the target data source name.
     * @param send
     *            the logical plan to send.
     */
    public SendPlan(String data_src_name, LogicalPlan send)
    {
        super.setExecutionDataSourceName(data_src_name);
        
        assert (send != null);
        m_send_plan = send;
    }
    
    /**
     * Default constructor.
     */
    @SuppressWarnings("unused")
    private SendPlan()
    {
        
    }
    
    /**
     * Gets the logical plan to send.
     * 
     * @return the logical plan to send.
     */
    public LogicalPlan getSendPlan()
    {
        return m_send_plan;
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        return Collections.<Variable> emptyList();
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        // From the parameters of the send logical plan, keep only the parameters that do not appear in the input info.
        List<Parameter> free_params = new ArrayList<Parameter>();
        for (Parameter free_param : m_send_plan.getFreeParametersUsed())
        {
            switch (free_param.getInstantiationMethod())
            {
                case ASSIGN:
                    if (!this.getInputVariables().contains(free_param.getVariable()))
                    {
                        free_params.add(free_param);
                    }
                    break;
                case COPY:
                    RelativeVariable param_var = free_param.getVariable();
                    boolean found = false;
                    for (Operator child : this.getChildren())
                    {
                        if (child instanceof Copy
                                && free_param.getDataSourceName().equals(((Copy) child).getTargetDataSourceName())
                                && free_param.getSchemaObjectName().equals(((Copy) child).getTargetSchemaObjectName())
                                && child.getOutputInfo().getVariables().contains(param_var))
                        {
                            found = true;
                            break;
                        }
                    }
                    
                    // FIXME Changed for test case of SemiJoin-BulkInsert
                    if (found == false)
                    {
                        free_params.add(free_param);
                    }
                    // assert (found);
                    break;
                default:
                    throw new AssertionError();
            }
        }
        
        return free_params;
    }
    
    @Override
    public List<Parameter> getBoundParametersUsed()
    {
        // FIXME
        // Needed during parsing of an existing plan
        // if (m_bound_params == null)
        // {
        this.setBoundParametersUsed();
        // }
        
        return m_bound_params;
    }
    
    /**
     * Sets the bounding parameters used by the operator.
     */
    private void setBoundParametersUsed()
    {
        m_bound_params = new ArrayList<Parameter>(m_send_plan.getFreeParametersUsed());
        m_bound_params.removeAll(this.getFreeParametersUsed());
    }
    
    @Override
    public List<LogicalPlan> getLogicalPlansUsed()
    {
        return Collections.<LogicalPlan> singletonList(m_send_plan);
    }
    
    @Override
    public void updateOutputInfo() throws QueryCompilationException
    {
        // No need to update the output info of the nested logical plan
        
        // Set the cardinality estimate based on the one of the root operator of the nested plan
        this.setCardinalityEstimate(m_send_plan.getRootOperator().getCardinalityEstimate());
        
        // Output info
        OutputInfo output_info = new OutputInfo();
        
        // Copy the output variables and keys of the nested plan root operator
        for (RelativeVariable var : m_send_plan.getRootOperator().getOutputInfo().getVariables())
        {
            // FIXME The provenance of the new variables should be the nested plan, not null
            output_info.add(var, null);
        }
        output_info.setKeyTerms(m_send_plan.getRootOperator().getOutputInfo().getKeyTerms());
        
        // Set the output_ordered flag of the output info
        output_info.setOutputOrdered(m_send_plan.getRootOperator().getOutputInfo().isOutputOrdered());
        // Set the actual output info
        this.setOutputInfo(output_info);
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        // Send plan operator is SQL compliant because we can bulk or parameter pass the result of the nested logical plan
        // FIXME Revise!!!
        return true;
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitSendPlan(this);
    }
    
    @Override
    public Operator copy()
    {
        SendPlan copy = new SendPlan(this.getExecutionDataSourceName(), m_send_plan.copy());
        super.copy(copy);
        
        return copy;
    }
}
