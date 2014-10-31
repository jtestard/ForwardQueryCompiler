package edu.ucsd.forward.query.logical;

import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

/**
 * Represents the operator navigating to a term. If the evaluation of the term is a collection, the operator does not iterate over
 * the tuples of the collection.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class Navigate extends AbstractUnaryOperator
{
    public static final String DEFAULT_ALIAS = "navigate";
    
    /**
     * The term.
     */
    private Term               m_term;
    
    /**
     * The alias of the term.
     */
    private RelativeVariable   m_alias_var;
    
    /**
     * Constructs an instance of a navigate operator.
     * 
     * @param alias
     *            the alias of the term.
     * @param term
     *            the term to navigate to.
     */
    public Navigate(String alias, Term term)
    {
        assert (term != null);
        m_term = term;
        
        String var_alias = (alias == null) ? DEFAULT_ALIAS : alias;
        
        m_alias_var = new RelativeVariable(var_alias);
        m_alias_var.setDefaultProjectAlias(m_term.getDefaultProjectAlias());
        m_alias_var.setLocation(m_term.getLocation());
        if (m_term.getType() != null)
        {
            m_alias_var.setType(m_term.getType());
        }
    }
    
    /**
     * Constructs an instance of a navigate operator.
     * 
     * @param alias
     *            the alias of the term.
     * @param term
     *            the term to navigate to.
     * @param child
     *            the child operator.
     */
    public Navigate(String alias, Term term, Operator child)
    {
        this(alias, term);
        
        assert (child != null);
        this.addChild(child);
    }
    
    /**
     * Default constructor.
     */
    @SuppressWarnings("unused")
    private Navigate()
    {
        
    }
    
    /**
     * Returns the term to navigate to.
     * 
     * @return a term.
     */
    public Term getTerm()
    {
        return m_term;
    }
    
    /**
     * Sets the term to navigate to.
     * 
     * @param term
     *            the term to set
     */
    public void setTerm(Term term)
    {
        assert term != null;
        m_term = term;
        m_alias_var.setDefaultProjectAlias(m_term.getDefaultProjectAlias());
        m_alias_var.setLocation(m_term.getLocation());
    }
    
    /**
     * Returns the relative variable corresponding to the term of the navigation.
     * 
     * @return a relative variable corresponding to the term of the navigation.
     */
    public RelativeVariable getAliasVariable()
    {
        return m_alias_var;
    }
    
    /**
     * Returns the name of the relative variable created by the term.
     * 
     * @return the name of the relative variable created by the term.
     */
    public String getAliasVariableName()
    {
        return m_alias_var.getName();
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        return m_term.getVariablesUsed();
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        return m_term.getFreeParametersUsed();
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
        
        // Compute the output info
        int binding_index = 0;
        
        // First using the input variables
        for (RelativeVariable in_var : this.getInputVariables())
        {
            output_info.add(in_var, in_var);
            binding_index++;
        }
        
        Type term_type = m_term.inferType(this.getChildren());
        m_alias_var.setType(term_type);
        m_alias_var.setBindingIndex(binding_index);
        output_info.add(m_alias_var, m_term);
        
        // Set the actual output info
        this.setOutputInfo(output_info);
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        return m_term.isDataSourceCompliant(metadata);
    }
    
    @Override
    public String toExplainString()
    {
        String out = super.toExplainString();
        
        out += " " + m_term + " as " + m_alias_var.getName();
        
        return out;
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitNavigate(this);
    }
    
    @Override
    public Operator copy()
    {
        Navigate copy = new Navigate(m_alias_var.getName(), (Term) m_term.copy());
        
        super.copy(copy);
        
        return copy;
    }
    
    @Override
    public Navigate copyWithoutType()
    {
        return new Navigate(m_alias_var.getName(), m_term.copyWithoutType());
    }
    
    /**
     * Returns the alias.
     * 
     * @return the alias
     */
    public String getAlias()
    {
        return m_alias_var.getName();
    }
}
