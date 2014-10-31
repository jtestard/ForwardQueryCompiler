package edu.ucsd.forward.query.ast.visitors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.ucsd.app2you.util.collection.Pair;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.TupleType.AttributeEntry;
import edu.ucsd.forward.exception.ExceptionMessages.QueryCompilation;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.AttributeReference;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.Project;
import edu.ucsd.forward.query.logical.term.AbsoluteVariable;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;

/**
 * The context of a logical plan maps attribute references to variables currently in the scope. Its purpose is to resolve variable
 * for the LogicalPlanBuilder.
 * 
 * @author Michalis Petropoulos
 * @author Romain Vernoux
 */
public final class LogicalPlanBuilderContext
{
    private LogicalPlanBuilder                        m_builder;
    
    /**
     * Maps attribute references to the corresponding variable currently in the scope. This map is used for exact match only. That
     * is, assuming there is a mapping "t -> v0" in this map and "t" has type tuple with an attribute "a", the attribute reference
     * "a" will find no match in this map.
     */
    private Map<AttributeReference, RelativeVariable> m_variable_exact_map;
    
    /**
     * Maps attribute references to the corresponding variable currently in the scope. This map is used for suffix match. That is,
     * assuming there is a mapping "t -> v0" in this map and "t" has type tuple with an attribute "a", the attribute reference "a"
     * will find at least the match "v0".
     */
    private Map<AttributeReference, RelativeVariable> m_variable_suffix_map;
    
    /**
     * Maps the string representation of grouping items to RelativeVariables in the scope after the GROUP BY clause. The value
     * expressions correspond to the grouping attributes and the carry-on items of the GROUP BY that stay in the scope.
     */
    private Map<String, RelativeVariable>             m_grouping_term_map;
    
    /**
     * This flag is enabled when the operators currently translated are above a GroupBy operator. This is important because the
     * variable resolution scheme changes after a GroupBy.
     */
    private boolean                                   m_above_group_by = false;
    
    /**
     * The list of Assigns in the context.
     */
    private Map<String, Assign>                       m_assign_map;
    
    /**
     * Constructor.
     * 
     * @param builder
     *            the corresponding logical plan builder.
     */
    protected LogicalPlanBuilderContext(LogicalPlanBuilder builder)
    {
        m_builder = builder;
        m_variable_suffix_map = new LinkedHashMap<AttributeReference, RelativeVariable>();
        m_variable_exact_map = new LinkedHashMap<AttributeReference, RelativeVariable>();
        m_grouping_term_map = new LinkedHashMap<String, RelativeVariable>();
        m_assign_map = new LinkedHashMap<String, Assign>();
    }
    
    /**
     * Gets the corresponding logical plan builder.
     * 
     * @return a logical plan builder.
     */
    protected LogicalPlanBuilder getLogicalPlanBuilder()
    {
        return m_builder;
    }
    
    /**
     * Sets the flag telling that the operators currently translated are above a GroupBy operator. This is important because the
     * variable resolution scheme changes after a GroupBy.
     * 
     * @param above_group_by
     *            the new flag value
     */
    protected void setAboveGroupBy(boolean above_group_by)
    {
        m_above_group_by = above_group_by;
    }
    
    /**
     * Gets the flag telling that the operators currently translated are above a GroupBy operator. This is important because the
     * variable resolution scheme changes after a GroupBy.
     * 
     * @return the flag
     */
    protected boolean isAboveGroupBy()
    {
        return m_above_group_by;
    }
    
    /**
     * Adds an assign target to the context.
     * 
     * @param target
     *            the target of an assign operator.
     * @param assign
     *            the assign operator
     */
    protected void addAssignTarget(String target, Assign assign)
    {
        assert target != null;
        assert assign != null;
        assert !m_assign_map.containsKey(target);
        m_assign_map.put(target, assign);
    }
    
    /**
     * Checks if the assign target is already contained in the context.
     * 
     * @param target
     *            the target to test
     * @return whether the assign target is already contained in the context.
     */
    protected boolean containsAssignTarget(String target)
    {
        assert target != null;
        return m_assign_map.containsKey(target);
    }
    
    /**
     * Gets the assign operator by giving its target name.
     * 
     * @param target
     *            the target
     * @return the assign operator.
     */
    protected Assign getAssign(String target)
    {
        assert target != null;
        return m_assign_map.get(target);
    }
    
    /**
     * Adds a mapping to the context from an attribute reference to a variable. The type of the variable should be set. The variable
     * will be used for exact match and suffix match.
     * 
     * @param attr_ref
     *            an attribute reference.
     * @param variable
     *            a variable.
     */
    protected void addMappingForSuffixMatch(AttributeReference attr_ref, RelativeVariable variable)
    {
        assert (variable.getType() != null);
        
        RelativeVariable previous = m_variable_suffix_map.put(attr_ref, variable);
        assert (previous == null);
        previous = m_variable_exact_map.put(attr_ref, variable);
        assert (previous == null);
    }
    
    /**
     * Adds a mapping to the context from an attribute reference to a variable. The type of the variable should be set. The variable
     * will be used for exact match only.
     * 
     * @param attr_ref
     *            an attribute reference.
     * @param variable
     *            a variable.
     */
    protected void addMappingForExactMatch(AttributeReference attr_ref, RelativeVariable variable)
    {
        assert (variable.getType() != null);
        
        RelativeVariable previous = m_variable_exact_map.put(attr_ref, variable);
        assert (previous == null);
    }
    
    /**
     * Adds a mapping from a group by item to a relative variable. The type of the variable should be set. The variable will be used
     * for exact match only.
     * 
     * @param item
     *            the grouping item
     * @param variable
     *            the relative variable
     */
    protected void addGroupingItem(ValueExpression item, RelativeVariable variable)
    {
        assert (variable.getType() != null);
        RelativeVariable previous = m_grouping_term_map.put(item.toExplainString(), variable);
        assert (previous == null);
    }
    
    /**
     * Removes all mappings from the context but keeps the WITH assignments.
     */
    protected void clear()
    {
        m_variable_suffix_map.clear();
        m_variable_exact_map.clear();
        m_grouping_term_map.clear();
    }
    
    /**
     * Gets the relative variables in the suffix map.
     * 
     * @return a collection of relative variables.
     */
    protected Collection<RelativeVariable> getVariablesForSuffixMatch()
    {
        return m_variable_suffix_map.values();
    }
    
    /**
     * Gets the relative variables in the exact map.
     * 
     * @return a collection of relative variables.
     */
    protected Collection<RelativeVariable> getVariablesForExactMatch()
    {
        return m_variable_exact_map.values();
    }
    
    /**
     * Searches for a variable in the context that matches exactly the input attribute reference.
     * 
     * @param attr_ref
     *            an attribute reference.
     * @return a pair containing the matched attribute reference and the matched corresponding variable, or null if there is no
     *         exact match.
     * @throws QueryCompilationException
     *             when there are more than one exact matches.
     */
    protected Pair<AttributeReference, RelativeVariable> resolveExact(AttributeReference attr_ref)
            throws QueryCompilationException
    {
        Pair<AttributeReference, RelativeVariable> match = null;
        
        for (Entry<AttributeReference, RelativeVariable> entry : m_variable_exact_map.entrySet())
        {
            if (entry.getKey().isPrefix(attr_ref))
            {
                // If there are two exact matches in the context, throw an error
                if (match != null)
                {
                    throw new QueryCompilationException(QueryCompilation.AMBIGUOUS_QUERY_PATH, attr_ref.getLocation(),
                                                        attr_ref.toString());
                }
                match = new Pair<AttributeReference, RelativeVariable>(entry.getKey(), entry.getValue());
            }
        }
        return match;
    }
    
    /**
     * Searches for a variable in the context for which the input attribute reference is a suffix, and returns either the
     * corresponding unique relative variable or a query path starting from a unique relative variable and navigates to the desired
     * attribute.
     * 
     * @param attr_ref
     *            an attribute reference.
     * 
     * @return a term, or null if there is no suffix match.
     * @throws QueryCompilationException
     *             when there are more than one suffix matches.
     */
    protected Term resolveSuffix(AttributeReference attr_ref) throws QueryCompilationException
    {
        List<String> attr_ref_steps = new ArrayList<String>(attr_ref.getPathSteps());
        String start_step = attr_ref_steps.get(0);
        Pair<AttributeReference, RelativeVariable> result_var = null;
        
        // First, try to see if there is an exact match for the first step
        result_var = this.resolveExact(attr_ref);
        if (result_var != null)
        {
            RelativeVariable matched_rel_var = result_var.getValue();
            List<String> matched_attr_ref_steps = result_var.getKey().getPathSteps();
            
            // If attr_ref has extra steps, return a query path
            if (matched_attr_ref_steps.size() < attr_ref_steps.size())
            {
                // Get the extra path steps
                List<String> extra_steps = attr_ref_steps.subList(matched_attr_ref_steps.size(), attr_ref_steps.size());
                
                QueryPath qp_result = new QueryPath(matched_rel_var, extra_steps);
                qp_result.setLocation(attr_ref.getLocation());
                qp_result.inferType(Collections.<Operator> emptyList());
                return qp_result;
            }
            else
            {
                assert (matched_attr_ref_steps.size() == attr_ref_steps.size());
                
                return matched_rel_var;
            }
        }
        
        // Second, try to see if the first step of the attribute reference is a WITH alias
        if (m_assign_map.get(start_step) != null)
        {
            String assign_name = m_assign_map.get(start_step).getTarget();
            
            AbsoluteVariable matched_abs_var = new AbsoluteVariable(DataSource.TEMP_ASSIGN_SOURCE, assign_name);
            matched_abs_var.setLocation(attr_ref.getLocation());
            matched_abs_var.inferType(Collections.<Operator> emptyList());
            
            // If attr_ref has extra steps, return a query path
            if (attr_ref_steps.size() > 1)
            {
                // Get the extra path steps
                List<String> extra_steps = attr_ref_steps.subList(1, attr_ref_steps.size());
                
                QueryPath qp_result = new QueryPath(matched_abs_var, extra_steps);
                qp_result.inferType(Collections.<Operator> emptyList());
                return qp_result;
            }
            else
            {
                return matched_abs_var;
            }
        }
        
        // Third, try to see if the first step of the attribute reference is an attribute of a tuple-typed variable.
        for (Entry<AttributeReference, RelativeVariable> entry : m_variable_suffix_map.entrySet())
        {
            AttributeReference entry_attr_ref = entry.getKey();
            RelativeVariable entry_rel_var = entry.getValue();
            
            if (!(entry_rel_var.getType() instanceof TupleType)) continue;
            
            TupleType tuple_type = (TupleType) entry_rel_var.getType();
            for (AttributeEntry attr_entry : tuple_type)
            {
                if (attr_entry.getName().equals(start_step))
                {
                    // No suffix match yet
                    if (result_var == null)
                    {
                        result_var = new Pair<AttributeReference, RelativeVariable>(entry_attr_ref, entry_rel_var);
                    }
                    // Two special cases for the resolution of variables in the ORDER BY clause. As in the SQL standard, the ORDER
                    // BY items can use attributes from above OR from below the Project operator. In this case, this method will
                    // match both, and we keep the one from below the Project.
                    else if (result_var.getKey().getPathSteps().get(0).equals(Project.PROJECT_ALIAS))
                    {
                        result_var = new Pair<AttributeReference, RelativeVariable>(entry_attr_ref, entry_rel_var);
                    }
                    else if (entry_attr_ref.getPathSteps().get(0).equals(Project.PROJECT_ALIAS))
                    {
                        continue;
                    }
                    // Two matches, raise an error
                    else
                    {
                        throw new QueryCompilationException(QueryCompilation.AMBIGUOUS_QUERY_PATH, attr_ref.getLocation(),
                                                            attr_ref.toString(), "");
                    }
                }
            }
        }
        
        // Return the result if a suffix match is found
        if (result_var != null)
        {
            QueryPath result = new QueryPath(result_var.getValue(), attr_ref_steps);
            result.inferType(Collections.<Operator> emptyList());
            return result;
        }
        else
        {
            // No exact match and no suffix match
            return null;
        }
    }
    
    /**
     * Searches for a variable in the context (above a GroupBy operator) that exactly matches the input value expression, and
     * returns the corresponding relative variable.
     * 
     * @param expression
     *            the value expression to look for.
     * @return the corresponding relative variable, or <code>null</code> if no match is found.
     */
    protected RelativeVariable matchValueExpressionAboveGroupBy(ValueExpression expression)
    {
        String expression_str = expression.toExplainString();
        RelativeVariable rel_var = m_grouping_term_map.get(expression_str);
        if(rel_var != null && expression instanceof AttributeReference)
        {
            AttributeReference attr_ref = (AttributeReference) expression;
            String last_step = attr_ref.getPathSteps().get(attr_ref.getPathSteps().size()-1);
            rel_var.setDefaultProjectAlias(last_step);
        }
        return rel_var;
    }
}
