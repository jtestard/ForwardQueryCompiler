package edu.ucsd.forward.query.logical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.type.NoSqlType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.plan.LogicalPlanUtil;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

/**
 * Represents the selection logical operator.
 * 
 * @author Michalis Petropoulos
 * 
 */

@SuppressWarnings("serial")
public class Select extends AbstractUnaryOperator implements ConditionOperator
{
    /**
     * Selection conditions.
     */
    private List<Term> m_conditions;
    
    /**
     * The merged selection condition.
     */
    private Term       m_merged_condition;
    
    /**
     * Initializes an instance of the operator.
     */
    public Select()
    {
        super();
        
        m_conditions = new ArrayList<Term>();
    }
    
    /**
     * Gets the conditions.
     * 
     * @return the conditions.
     */
    public List<Term> getConditions()
    {
        return m_conditions;
    }
    
    /**
     * Gets the merged condition.
     * 
     * @return the merged condition.
     */
    public Term getMergedCondition()
    {
        if (m_merged_condition == null)
        {
            m_merged_condition = LogicalPlanUtil.mergeTerms(m_conditions);
        }
        
        return m_merged_condition;
    }
    
    public void setMergedCondition(Term condition)
    {
        assert condition != null;
        m_merged_condition = condition;
    }
    
    /**
     * Adds a selection condition.
     * 
     * @param condition
     *            the selection condition to add.
     */
    public void addCondition(Term condition)
    {
        assert (condition != null);
        m_conditions.add(condition);
        m_merged_condition = null;
    }
    
    /**
     * Removes a selection condition.
     * 
     * @param condition
     *            the selection condition to remove.
     */
    public void removeCondition(Term condition)
    {
        assert (m_conditions.remove(condition));
        m_merged_condition = null;
    }
    
    public void clearCondition()
    {
        m_conditions.clear();
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        // The conditions might be empty as a logical plan is being constructed.
        if (this.getConditions().size() > 0)
        {
            return this.getMergedCondition().getVariablesUsed();
        }
        
        return Collections.<Variable> emptyList();
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        return this.getMergedCondition().getFreeParametersUsed();
    }
    
    @Override
    public List<Parameter> getBoundParametersUsed()
    {
        return Collections.emptyList();
    }
    
    @Override
    public void updateOutputInfo() throws QueryCompilationException
    {
        // The conditions might be empty as a logical plan is being constructed.
        if (this.getConditions().size() > 0)
        {
            Type condition_type = this.getMergedCondition().inferType(this.getChildren());
            assert (condition_type instanceof BooleanType);
        }
        
        // Output info
        OutputInfo output_info = new OutputInfo();
        
        // Copy the input variables and keys
        for (RelativeVariable var : this.getInputVariables())
        {
            output_info.add(var, var);
        }
        output_info.setKeyTerms(getChild().getOutputInfo().getKeyTerms());
        
        output_info.setSingleton(getChild().getOutputInfo().isSingleton());
        
        // Set the actual output info
        this.setOutputInfo(output_info);
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        boolean compliant = this.getMergedCondition().isDataSourceCompliant(metadata);
        
        return (super.isDataSourceCompliant(metadata) && compliant);
    }
    
    @Override
    public String toExplainString()
    {
        String str = super.toExplainString() + " ";
        str += this.getMergedCondition().toExplainString();
        
        return str;
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitSelect(this);
    }
    
    @Override
    public Operator copy()
    {
        Select copy = new Select();
        super.copy(copy);
        
        for (Term condition : m_conditions)
            copy.addCondition(condition.copy());
        
        return copy;
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br>
     * {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.query.logical.AbstractOperator#copyWithoutType()
     */
    @Override
    public Select copyWithoutType()
    {
        Select copy = new Select();
        
        for (Term condition : m_conditions)
            copy.addCondition(condition.copyWithoutType());
        
        return copy;
    }
}
