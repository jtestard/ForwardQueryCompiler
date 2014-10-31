package edu.ucsd.forward.query.logical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.query.explain.ExplanationPrinter;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;

/**
 * Represents the output information of an operator. An output info consists of the output type of the operator and captures the
 * provenance of the attribute names in the output collection tuple type of an operator in a query plan. Provenance maps a variable,
 * which corresponds to an attribute name in the output collection tuple type of the operator, to a term. The provenance is null
 * when a variable is the result of a nested plan. The size of the provenance map must be equal to the arity of the output
 * collection tuple type of the operator.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class OutputInfo implements ExplanationPrinter, Serializable
{
    private Map<RelativeVariable, Term>   m_provenance;
    
    /**
     * Maps the names of the attributes in the provenance to their variables.
     */
    private Map<String, RelativeVariable> m_provenance_attr_to_var;
    
    /**
     * Maps the attribute names of the output collections type's tuple type to variables.
     */
    private Map<String, RelativeVariable> m_attr_to_var;
    
    private List<RelativeVariable>        m_var_indices;
    
    private List<Term>                    m_key_terms;
    
    private Set<RelativeVariable>         m_key_vars;
    
    /**
     * Whether the output type (collection) is known to be singleton.
     */
    private boolean                       m_singleton;
    
    /**
     * Whether the output is ordered.
     */
    private boolean                       m_output_ordered   = false;
    
    /**
     * Constructs an instance of an output info. By default it is NOT singleton.
     */
    public OutputInfo()
    {
        m_provenance = new LinkedHashMap<RelativeVariable, Term>();
        m_attr_to_var = new LinkedHashMap<String, RelativeVariable>();
        m_provenance_attr_to_var = new LinkedHashMap<String, RelativeVariable>();
        m_var_indices = new ArrayList<RelativeVariable>();
        m_key_terms = new ArrayList<Term>();
        m_key_vars = new LinkedHashSet<RelativeVariable>();
        m_singleton = false;
    }
    
    /**
     * Puts a <variable, term> provenance mapping into the output info. The method assumes there is no existing mapping using the
     * same variable.
     * 
     * @param variable
     *            the variable of the mapping.
     * @param term
     *            the term of the mapping.
     */
    public void add(RelativeVariable variable, Term term)
    {
        Term previous_term = m_provenance.put(variable, term);
        assert (previous_term == null);
        RelativeVariable previous_variable = m_attr_to_var.put(variable.getName(), variable);
        assert (previous_variable == null);
        
        m_var_indices.add(variable);
    }
    
    /**
     * Removes a given variable from the output info. The method assumes the input variable is part of the output info.
     * 
     * @param variable
     *            the variable of the mapping to remove.
     * @return the term of the removed mapping.
     */
    public Term remove(Variable variable)
    {
        Term term = m_provenance.remove(variable);
        assert (term != null);
        
        RelativeVariable rvariable = m_attr_to_var.remove(variable.getName());
        assert (rvariable != null);
        boolean removed = m_var_indices.remove(variable);
        assert removed;
        
        return term;
    }
    
    /**
     * Removes a given variable from the output info but retains it in the provenance.
     * 
     * @param variable
     *            the variable of the mapping to remove.
     * @return the term of the removed mapping.
     */
    public void removeAndRetainProvenance(Variable variable)
    {
        RelativeVariable rvariable = m_attr_to_var.remove(variable.getName());
        assert (rvariable != null);
        boolean removed = m_var_indices.remove(variable);
        assert removed;
    }
    
    /**
     * Determines if a given variable is contained in the output info.
     * 
     * @param variable
     *            a variable.
     * @return true if the given variable is part of the output info; otherwise, false.
     */
    public boolean contains(Variable variable)
    {
        return m_var_indices.contains(variable);
    }
    
    /**
     * Adds a new provenance term.
     * 
     * @param variable
     *            a variable in the input type.
     * @param term
     *            a term in the output type.
     */
    public void addProvenanceTerm(RelativeVariable variable, Term term)
    {
        m_provenance.put(variable, term);
        m_provenance_attr_to_var.put(variable.getName(), variable);
    }
    
    /**
     * Returns the provenance term for a given variable.
     * 
     * @param variable
     *            a variable in the output type.
     * @return a term, or null if the term has no provenance.
     */
    public Term getProvenanceTerm(Variable variable)
    {
        Term term = m_provenance.get(variable);
        
        return term;
    }
    
    /**
     * Returns the provenance term for a given variable.
     * 
     * @param variable
     *            a variable in the output type.
     * @return a term, or null if the term has no provenance.
     */
    public Variable getProvenanceVariable(Term term)
    {
        for (RelativeVariable var : m_provenance.keySet())
        {
            if (m_provenance.get(var).equals(term)) return var;
        }
        
        return null;
    }
    
    /**
     * Determines if the provenance of the given variable is present in the output info.
     * 
     * @param var
     *            a variable.
     * @return true if the provenance of the given variable is present in the output info; ow false.
     */
    public boolean hasProvenanceVariable(RelativeVariable var)
    {
        return m_provenance.containsKey(var);
    }
    
    /**
     * Determines if the given Term is the provenance of a variable in the output.
     * 
     * @param var
     *            a variable.
     * @return true if the term is the provenance term of a variable
     */
    public boolean hasProvenanceTerm(Term term)
    {
        return m_provenance.containsValue(term);
    }
    
    /**
     * Returns the variable for a given output attribute name.
     * 
     * @param attr_name
     *            an attribute name in the output type.
     * @return a variable.
     */
    public RelativeVariable getVariable(String attr_name)
    {
        RelativeVariable var = m_attr_to_var.get(attr_name);
        
        assert (var != null);
        
        return var;
    }
    
    /**
     * Returns the variable for a given input attribute name.
     * 
     * @param attr_name
     *            an attribute name in the input type.
     * @return a variable.
     */
    public RelativeVariable getProvenanceVariable(String attr_name)
    {
        RelativeVariable var = m_provenance_attr_to_var.get(attr_name);
        
        assert (var != null);
        
        return var;
    }
    
    /**
     * Returns whether there is a variable of the given name
     * 
     * @param attr_name
     *            an attribute name in the output type.
     * @return whether there is a variable of the given name
     */
    public boolean hasVariable(String attr_name)
    {
        return m_attr_to_var.containsKey(attr_name);
    }
    
    public boolean hasMappedVariable(String attr_name)
    {
        for (RelativeVariable var : m_attr_to_var.values())
        {
            if (var.getDefaultProjectAlias().equals(attr_name)) return true;
        }
        return false;
    }
    
    /**
     * Given a variable, returns the index position of the variable in an input binding. The method assumes that the variable
     * appears as a key in the provenance map.
     * 
     * @param variable
     *            a variable.
     * @return the index position of the variable in an input binding.
     */
    public int getVariableIndex(Variable variable)
    {
        return m_var_indices.indexOf(variable);
    }
    
    /**
     * Gets the variable at the given index position.
     * 
     * @param index
     *            the index to find
     * @return the variable at the given index position.
     */
    public Variable getVariable(int index)
    {
        return m_var_indices.get(index);
    }
    
    /**
     * Gets all variables contained in the output info.
     * 
     * @return all the variables contained in the output info.
     */
    public Set<RelativeVariable> getVariables()
    {
        return (m_var_indices.isEmpty())
                ? Collections.<RelativeVariable> emptySet()
                : Collections.unmodifiableSet(new LinkedHashSet<RelativeVariable>(m_var_indices));
    }
    
    /**
     * Gets all variables contained in the provenance info.
     * 
     * @return all the variables contained in the provenance info.
     */
    public Set<RelativeVariable> getProvenanceVariables()
    {
        return Collections.unmodifiableSet(m_provenance.keySet());
    }
    
    /**
     * Gets all attribute names of the variables contained in the output info.
     * 
     * @return all attribute names of the variables contained in the output info.
     */
    public Set<String> getAttributeNames()
    {
        return (m_var_indices.isEmpty()) ? Collections.<String> emptySet() : Collections.unmodifiableSet(m_attr_to_var.keySet());
    }
    
    public Collection<Term> getProvenanceTerms()
    {
        return Collections.unmodifiableCollection(m_provenance.values());
    }
    
    /**
     * Returns the size of the output info.
     * 
     * @return the size of the output info.
     */
    public int size()
    {
        return m_var_indices.size();
    }
    
    /**
     * Determines whether the output collection type has a local primary key constraint.
     * 
     * @return true, if the output collection type has a local primary key constraint; otherwise, false.
     */
    public boolean hasKeyTerms()
    {
        return !m_key_terms.isEmpty();
    }
    
    /**
     * Returns the key terms.
     * 
     * @return the key terms, if any; otherwise, the empty list.
     */
    public List<Term> getKeyTerms()
    {
        return Collections.unmodifiableList(m_key_terms);
    }
    
    /**
     * Returns the key variables.
     * 
     * @return the key variables, if any; otherwise, the empty set.
     */
    public Set<RelativeVariable> getKeyVars()
    {
        return Collections.unmodifiableSet(m_key_vars);
    }
    
    /**
     * Determines whether the output collection type has a local primary key constraint.
     * 
     * @return true, if the output collection type has a local primary key constraint; otherwise, false.
     */
    public boolean hasKeyVars()
    {
        return !m_key_vars.isEmpty();
    }
    
    /**
     * Sets the key variables. The given keys will be "addAll" to this class's internal collection. Each variable must appear in the
     * variable indices list.
     * 
     * @param keys
     *            the key variables to set.
     */
    public void setKeyVars(Set<RelativeVariable> keys)
    {
        for (RelativeVariable key : keys)
        {
            assert m_var_indices.contains(key);
        }
        m_key_vars.clear();
        m_key_vars.addAll(keys);
    }
    
    /**
     * Adds a key variable. The variable must appear in the variable indices list.
     * 
     * @param key
     *            the key variable to add.
     */
    public void addKeyVar(RelativeVariable key)
    {
        assert m_var_indices.contains(key);
        m_key_vars.add(key);
    }
    
    /**
     * Clears all the key variables.
     */
    public void clearKeyVars()
    {
        m_key_vars.clear();
    }
    
    /**
     * Sets the key terms. The given keys will be "addAll" to this class's internal collection. I.e., the given list will not be
     * used directly.
     * 
     * @param keys
     *            the key variables to set.
     */
    public void setKeyTerms(List<? extends Term> keys)
    {
        for (Term key : keys)
        {
            List<Variable> var_used = key.getVariablesUsed();
            // Key terms should use only one variable
            assert (var_used.size() == 1);
            assert (m_var_indices.containsAll(var_used));
        }
        m_key_terms.clear();
        m_key_terms.addAll(keys);
    }
    
    /**
     * Returns whether the output collection type is known to be singleton.
     * 
     * @return whether the output collection type is known to be singleton.
     */
    public boolean isSingleton()
    {
        return m_singleton;
    }
    
    /**
     * Determines whether the output collection type is known to be singleton.
     * 
     * @param singleton
     *            whether the output collection type is known to be singleton.
     */
    public void setSingleton(boolean singleton)
    {
        m_singleton = singleton;
    }
    
    /**
     * Set the flag to tell if the output is ordered.
     * 
     * @param flag
     *            should be <code>true</code> if the output is ordered, <code>false</code> otherwise.
     */
    public void setOutputOrdered(boolean flag)
    {
        m_output_ordered = flag;
    }
    
    /**
     * Tell if the output is ordered.
     * 
     * @return <code>true</code> if the output is ordered, <code>false</code> otherwise.
     */
    public boolean isOutputOrdered()
    {
        return m_output_ordered;
    }
    
    @Override
    public String toExplainString()
    {
        Type attr_type;
        
        String str = "[ ";
        for (Variable variable : this.getVariables())
        {
            attr_type = variable.getType();
            
            String attr_type_name = attr_type.getClass().getName();
            attr_type_name = attr_type_name.substring(attr_type_name.lastIndexOf('.') + 1, attr_type_name.length());
            str += variable + " (" + attr_type_name + ")" + ", ";
        }
        if (!this.getVariables().isEmpty()) str = str.substring(0, str.length() - 2);
        
        return str + " ]";
    }
    
    /**
     * Creates the corresponding collection type of this output info. CAUTION: This is said to be inefficient and therefore not
     * provided initially. So justify your use case first.
     * 
     * Also the collection type does NOT have primary key constraint.
     * 
     * @return a collection type.
     */
    public CollectionType getType()
    {
        TupleType out_tuple_type = new TupleType();
        for (RelativeVariable rel_var : getVariables())
        {
            out_tuple_type.setAttribute(rel_var.getName(), TypeUtil.cloneNoParent(rel_var.getType()));
        }
        
        return new CollectionType(out_tuple_type);
    }
    
}
