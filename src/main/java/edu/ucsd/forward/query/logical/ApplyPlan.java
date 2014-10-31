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
 * Represents an operator that constructs nested collection values. The operator contains a logical plan that generates a nested
 * collection value when applied to every binding of the child operator. The nested logical plan can have nested apply plan
 * operators.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class ApplyPlan extends AbstractUnaryOperator implements NestedPlansOperator
{
    /**
     * The logical plan to apply.
     */
    private LogicalPlan      m_apply_plan;
    
    /**
     * The alias of the nested collection value.
     */
    private RelativeVariable m_alias_var;
    
    /**
     * The bound parameters.
     */
    private List<Parameter>  m_bound_params;
    
    /**
     * The execution condition. If it evaluates to <code>true</code>, the logical plan is evaluated. Otherwise, the execution is
     * short-circuited and a <code>null</code> value is returned.
     */
    private Term             m_execution_condition;
    
    /**
     * Initializes an instance of the operator.
     * 
     * @param alias
     *            the alias of the logical plan.
     * @param apply
     *            the logical plan to apply.
     */
    public ApplyPlan(String alias, LogicalPlan apply)
    {
        super();
        
        assert (alias != null);
        m_alias_var = new RelativeVariable(alias);
        
        assert (apply != null);
        m_apply_plan = apply;
    }
    
    /**
     * Default constructor.
     */
    @SuppressWarnings("unused")
    private ApplyPlan()
    {
        super();
    }
    
    /**
     * Returns the logical plan to apply, or <code>null</code> if it is not set yet.
     * 
     * @return the logical plan to apply.
     */
    public LogicalPlan getApplyPlan()
    {
        return m_apply_plan;
    }
    
    /**
     * Returns the alias variable of the logical plan.
     * 
     * @return an alias variable.
     */
    public RelativeVariable getAliasVariable()
    {
        return m_alias_var;
    }
    
    /**
     * Returns the alias of the logical plan.
     * 
     * @return an alias.
     */
    public String getAlias()
    {
        return m_alias_var.getName();
    }
    
    /**
     * Tells if the ApplyPlan has an execution condition, or always executes.
     * 
     * @return <code>true</code> if the ApplyPlan has an execution condition, and <code>false</code> otherwise.
     */
    public boolean hasExecutionCondition()
    {
        return m_execution_condition != null;
    }
    
    /**
     * Returns the execution condition.
     * 
     * @return the execution condition if the ApplyPlan has an execution condition, and <code>null</code> otherwise.
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
        // From the parameters of the apply logical plan, keep only the parameters that do not appear in the input info.
        List<Parameter> free_params = new ArrayList<Parameter>();
        for (Parameter free_param : m_apply_plan.getFreeParametersUsed())
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
        m_bound_params = new ArrayList<Parameter>(m_apply_plan.getFreeParametersUsed());
        m_bound_params.removeAll(this.getFreeParametersUsed());
    }
    
    @Override
    public List<LogicalPlan> getLogicalPlansUsed()
    {
        return Collections.<LogicalPlan> singletonList(m_apply_plan);
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
        
        // Compute the type of the variable
        m_alias_var.setType(m_apply_plan.getOutputType());
        
        // If there is an execution condition, infer its type
        if (m_execution_condition != null)
        {
            m_execution_condition.inferType(Arrays.asList(this.getChild()));
        }
        
        // FIXME The provenance of the new variables should be the nested plan, not null
        output_info.add(m_alias_var, null);
        
        output_info.setSingleton(this.getChild().getOutputInfo().isSingleton());
        
        // Set the actual output info
        this.setOutputInfo(output_info);
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        switch (metadata.getDataModel())
        {
            case SQLPLUSPLUS:
                return super.isDataSourceCompliant(metadata);
            case RELATIONAL:
                // Never compliant with relational data sources
                return false;
            default:
                throw new AssertionError();
        }
    }
    
    @Override
    public String toExplainString()
    {
        String str = super.toExplainString() + " -> " + m_alias_var;
        if (hasExecutionCondition())
        {
            str += "; exec_condition: " + getExecutionCondition().toExplainString();
        }
        return str;
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitApplyPlan(this);
    }
    
    @Override
    public Operator copy()
    {
        ApplyPlan copy = new ApplyPlan(m_alias_var.getName(), m_apply_plan.copy());
        
        if (hasExecutionCondition())
        {
            copy.setExecutionCondition(getExecutionCondition());
        }
        
        super.copy(copy);
        
        return copy;
    }
    
}
