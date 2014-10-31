package edu.ucsd.forward.query.logical;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

/**
 * Represents an operator that checks for every binding of the child operator whether an inner logical plan returns at least one
 * binding, and adds a boolean-typed attribute to the output. The inner logical plan can have inner exists operators.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class Exists extends AbstractUnaryOperator implements NestedPlansOperator
{
    /**
     * The logical plan to check.
     */
    private LogicalPlan     m_exists_plan;
    
    /**
     * The variable name for the boolean value.
     */
    private String          m_var_name;
    
    /**
     * The bound parameters.
     */
    private List<Parameter> m_bound_params;
    
    /**
     * The execution condition. If it evaluates to <code>true</code>, the logical plan is evaluated. Otherwise, the execution is
     * short-circuited and a <code>null</code> value is returned.
     */
    private Term            m_execution_condition;
    
    /**
     * Initializes an instance of the operator.
     * 
     * @param var_name
     *            the variable name for the boolean value.
     * @param exists_plan
     *            the logical plan to check.
     */
    public Exists(String var_name, LogicalPlan exists_plan)
    {
        assert (var_name != null);
        m_var_name = var_name;
        
        assert (exists_plan != null);
        m_exists_plan = exists_plan;
    }
    
    /**
     * Default constructor.
     */
    @SuppressWarnings("unused")
    private Exists()
    {
        
    }
    
    /**
     * Returns the exists plan.
     * 
     * @return the exists plan
     */
    public LogicalPlan getExistsPlan()
    {
        return m_exists_plan;
    }
    
    /**
     * Returns the variable name for the boolean value.
     * 
     * @return a variable name.
     */
    public String getVariableName()
    {
        return m_var_name;
    }
    
    /**
     * Tells if the Exists has an execution condition, or always executes.
     * 
     * @return <code>true</code> if the Exists has an execution condition, and <code>false</code> otherwise.
     */
    public boolean hasExecutionCondition()
    {
        return m_execution_condition != null;
    }
    
    /**
     * Returns the execution condition.
     * 
     * @return the execution condition if the Exists has an execution condition, and <code>null</code> otherwise.
     */
    public Term getExecutionCondition()
    {
        return m_execution_condition;
    }
    
    /**
     * Sets the execution condition.
     * 
     * @param execution_condition
     *            the condition to set
     */
    public void setExecutionCondition(Term execution_condition)
    {
        assert (execution_condition != null);
        assert (execution_condition.getType() == null || execution_condition.getType() instanceof BooleanType);
        
        this.m_execution_condition = execution_condition;
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        if (hasExecutionCondition())
        {
            return getExecutionCondition().getVariablesUsed();
        }
        else
        {
            return Collections.emptyList();
        }
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        // From the parameters of the exists logical plan, keep only the parameters that do not appear in the input info.
        List<Parameter> free_params = new ArrayList<Parameter>();
        for (Parameter free_param : m_exists_plan.getFreeParametersUsed())
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
        // Needed during parsing of an existing plan
        if (m_bound_params == null)
        {
            this.setBoundParametersUsed();
        }
        
        return m_bound_params;
    }
    
    /**
     * Sets the bounding parameters used by the operator.
     */
    private void setBoundParametersUsed()
    {
        m_bound_params = new ArrayList<Parameter>(m_exists_plan.getFreeParametersUsed());
        m_bound_params.removeAll(this.getFreeParametersUsed());
    }
    
    @Override
    public List<LogicalPlan> getLogicalPlansUsed()
    {
        return Collections.<LogicalPlan> singletonList(m_exists_plan);
    }
    
    @Override
    public void updateOutputInfo() throws QueryCompilationException
    {
        // No need to update the output info of the nested logical plan
        
        // Set the bound parameters
        this.setBoundParametersUsed();
        
        // Validate the bound parameters
        for (Parameter param : this.getBoundParametersUsed())
            param.getTerm().inferType(this.getChildren());
        
        // Output info
        OutputInfo output_info = new OutputInfo();
        
        // Copy the input variables and keys WITHOUT the new variable
        for (RelativeVariable var : this.getInputVariables())
        {
            output_info.add(var, var);
        }
        output_info.setKeyTerms(this.getChild().getOutputInfo().getKeyTerms());
        
        // If there is an execution condition, infer its type
        if (m_execution_condition != null)
        {
            m_execution_condition.inferType(Arrays.asList(this.getChild()));
        }
        
        // Add output info entry for boolean attribute WITH the new variable
        RelativeVariable new_var = new RelativeVariable(m_var_name);
        new_var.setType(new BooleanType());
        // FIXME The provenance of the new variable should be the nested plan, not null
        output_info.add(new_var, null);
        
        // Set the actual output info
        this.setOutputInfo(output_info);
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        boolean compliant = super.isDataSourceCompliant(metadata);
        switch (metadata.getDataModel())
        {
            case SQLPLUSPLUS:
                return compliant;
            case RELATIONAL:
                return compliant && m_exists_plan.isDataSourceCompliant(metadata);
            default:
                throw new AssertionError();
        }
    }
    
    @Override
    public String toExplainString()
    {
        String str = super.toExplainString() + " -> " + m_var_name;
        if (hasExecutionCondition())
        {
            str += "; exec_condition: " + getExecutionCondition().toExplainString();
        }
        return str;
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitExists(this);
    }
    
    @Override
    public Operator copy()
    {
        Exists copy = new Exists(m_var_name, m_exists_plan.copy());
        if (hasExecutionCondition())
        {
            copy.setExecutionCondition(getExecutionCondition());
        }
        super.copy(copy);
        
        return copy;
    }
    
}
