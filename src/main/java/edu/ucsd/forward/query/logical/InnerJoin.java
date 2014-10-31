package edu.ucsd.forward.query.logical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
 * Represents the inner join logical operator.
 * 
 * @author Michalis Petropoulos
 */
@SuppressWarnings("serial")
public class InnerJoin extends AbstractOperator implements ConditionOperator
{
    /**
     * Join conditions.
     */
    private List<Term> m_conditions;
    
    /**
     * The merged join condition.
     */
    private Term       m_merged_condition;
    
    /**
     * Initializes an instance of the operator.
     */
    public InnerJoin()
    {
        super();
        
        m_conditions = new ArrayList<Term>();
    }
    
    @Override
    public void addCondition(Term condition)
    {
        assert condition != null;
        m_conditions.add(condition);
        m_merged_condition = null;
    }
    
    @Override
    public void removeCondition(Term condition)
    {
        assert (m_conditions.remove(condition));
        m_merged_condition = null;
    }
    
    @Override
    public List<Term> getConditions()
    {
        return m_conditions;
    }
    
    @Override
    public Term getMergedCondition()
    {
        if (m_merged_condition == null)
        {
            m_merged_condition = LogicalPlanUtil.mergeTerms(m_conditions);
        }
        
        return m_merged_condition;
    }
    
    @Override
    public void updateOutputInfo() throws QueryCompilationException
    {
        for (Term condition : m_conditions)
        {
            Type condition_type = condition.inferType(this.getChildren());
            assert (condition_type instanceof BooleanType || condition_type instanceof NoSqlType);
        }
        
        // Output info
        OutputInfo output_info = new OutputInfo();
        
        // Copy the input variables
        for (RelativeVariable var : this.getInputVariables())
        {
            output_info.add(var, var);
        }
        // FIXME Add key inference
        
        // Set the actual output info
        this.setOutputInfo(output_info);
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        List<Variable> result = new ArrayList<Variable>();
        
        for (Term condition : m_conditions)
        {
            result.addAll(condition.getVariablesUsed());
        }
        
        return result;
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        List<Parameter> result = new ArrayList<Parameter>();
        
        for (Term condition : m_conditions)
        {
            result.addAll(condition.getFreeParametersUsed());
        }
        
        return result;
    }
    
    @Override
    public List<Parameter> getBoundParametersUsed()
    {
        return Collections.emptyList();
    }
    
    @Override
    public String toExplainString()
    {
        String str = super.toExplainString() + " on ";
        
        for (Term condition : m_conditions)
        {
            str += condition.toExplainString() + " and ";
        }
        if(!m_conditions.isEmpty())
        {
            str = str.substring(0, str.length() - 5);
        }
        
        return str;
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitInnerJoin(this);
    }
    
    @Override
    public Operator copy()
    {
        InnerJoin copy = new InnerJoin();
        super.copy(copy);
        
        for (Term term : m_conditions)
            copy.addCondition(term.copy());
        
        return copy;
    }
    
    @Override
    public InnerJoin copyWithoutType()
    {
        InnerJoin copy = new InnerJoin();
        
        for (Term term : m_conditions)
            copy.addCondition(term.copyWithoutType());
        
        return copy;
    }
}
