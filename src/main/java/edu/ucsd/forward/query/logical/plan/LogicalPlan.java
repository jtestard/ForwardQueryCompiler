package edu.ucsd.forward.query.logical.plan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.constraint.LocalPrimaryKeyConstraint;
import edu.ucsd.forward.data.constraint.SingletonConstraint;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.query.DataSourceCompliance;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.explain.ExplanationPrinter;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.ParameterUsage;
import edu.ucsd.forward.query.logical.term.AbsoluteVariable;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Variable;

/**
 * Represents a logical query plan.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class LogicalPlan implements ParameterUsage, DataSourceCompliance, ExplanationPrinter, Serializable
{
    /**
     * The root operator of the logical query plan.
     */
    private Operator               m_root;
    
    /**
     * The free parameters of the logical plan.
     */
    private List<Parameter>        m_free_params;
    
    /**
     * The absolute variables of the logical plan.
     */
    private List<AbsoluteVariable> m_absolute_vars;
    
    /**
     * The list of assignments.
     */
    private List<Assign>           m_assigns;
    
    /**
     * Whether the logical plan wraps a term.
     */
    private boolean                m_wrapping;
    
    /**
     * Determines whether the logical plan is nested within another logical plan.
     */
    private boolean                m_is_nested;
    
    private Set<KeyGroup>          m_key_groups;
    
    /**
     * Default constructor.
     */
    public LogicalPlan()
    {
        
    }
    
    /**
     * Constructs an instance of the logical query plan given a root operator.
     * 
     * @param root
     *            the root operator of the query plan.
     */
    public LogicalPlan(Operator root)
    {
        assert (root != null);
        m_root = root;
        
        m_wrapping = false;
        
        m_is_nested = false;
        
        m_free_params = new ArrayList<Parameter>();
        m_absolute_vars = new ArrayList<AbsoluteVariable>();
        m_key_groups = new HashSet<KeyGroup>();
        m_assigns = new ArrayList<Assign>();
    }
    
    /**
     * Adds an assign operator to plan. The operator is added to the list in the position that is after all the dependent assign
     * operators, so that this no referencing scan operators in the assigned plan that references any assign operator later in the
     * list.
     * 
     * @param assign
     *            the assign operator to add.
     */
    public void addAssignOperator(Assign assign)
    {
        // FIXME: adds the checking as the javadoc says.
        assert assign != null;
        m_assigns.add(assign);
    }
    
    /**
     * Gets the assign operators configured on this plan.
     * 
     * @return the assign operators.
     */
    public List<Assign> getAssigns()
    {
        return Collections.unmodifiableList(m_assigns);
    }
    
    /**
     * Gets the assign operator by name.
     * 
     * @param name
     *            the name
     * @return the assign operator. null otherwise.
     */
    public Assign getAssign(String name)
    {
        for (Assign assign : m_assigns)
        {
            if (assign.getTarget().equals(name))
            {
                return assign;
            }
        }
        return null;
    }
    
    /**
     * Clear all the assign operators in the plan.
     */
    public void clearAssigns()
    {
        m_assigns.clear();
    }
    
    /**
     * Returns the root operator of the logical query plan.
     * 
     * @return the root operator.
     */
    public Operator getRootOperator()
    {
        return m_root;
    }
    
    /**
     * Sets the root operator of the logical query plan.
     * 
     * @param root
     *            the root operator.
     */
    public void setRootOperator(Operator root)
    {
        assert (root != null);
        m_root = root;
    }
    
    /**
     * Returns the collection type of the logical plan.
     * 
     * @return the collection type of the logical plan.
     */
    public CollectionType getOutputType()
    {
        CollectionType type = new CollectionType();
        // Normal form assumption
        assert(m_root.getOutputInfo().getVariables().size() == 1);
        RelativeVariable rel_var = (RelativeVariable) m_root.getOutputInfo().getVariable(0);
        type.setChildrenType(TypeUtil.cloneNoParent(rel_var.getType()));
        
        // Commented out during Normal Form refactoring
        // if (m_root.getOutputInfo().hasKeyVars())
        // {
        // List<Type> keys = new ArrayList<Type>();
        // for (RelativeVariable key : m_root.getOutputInfo().getKeyVars())
        // {
        // Type key_type = type.getTupleType().getAttribute(((RelativeVariable) key).getDefaultProjectAlias());
        // if (!(key_type != null && key_type instanceof ScalarType))
        // {
        // throw new AssertionError();
        // }
        // keys.add(key_type);
        // }
        // type.addConstraint(new LocalPrimaryKeyConstraint(type, keys));
        // }
        
        if (m_root.getOutputInfo().isSingleton())
        {
            new SingletonConstraint(type);
        }
        
        type.setOrdered(m_root.getOutputInfo().isOutputOrdered());
        
        return type;
    }
    
    /**
     * Returns whether the logical plan wraps a term.
     * 
     * @return true, if the logical plan wraps a term; otherwise, false.
     */
    public boolean isWrapping()
    {
        return m_wrapping;
    }
    
    /**
     * Sets whether the logical plan wraps a term.
     * 
     * @param wrapping
     *            whether the logical plan wraps a term.
     */
    public void setWrapping(boolean wrapping)
    {
        m_wrapping = wrapping;
    }
    
    /**
     * Returns whether the logical plan is nested within another logical plan.
     * 
     * @return true, if the logical plan is nested within another logical plan; otherwise, false.
     */
    public boolean isNested()
    {
        return m_is_nested;
    }
    
    /**
     * Sets whether the logical plan is nested within another logical plan.
     * 
     * @param is_nested
     *            whether the logical plan is nested within another logical plan.
     */
    public void setNested(boolean is_nested)
    {
        m_is_nested = is_nested;
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        return m_free_params;
    }
    
    @Override
    public List<Parameter> getBoundParametersUsed()
    {
        return Collections.emptyList();
    }
    
    /**
     * Returns the list of absolute variables used by operators in the logical plan. Duplicates are allowed.
     * 
     * @return the list of absolute variables used by operators in the logical plan.
     */
    public List<AbsoluteVariable> getAbsoluteVariablesUsed()
    {
        return m_absolute_vars;
    }
    
    /**
     * Recursively updates the output types of all operators in the logical query plan in a bottom-up fashion. In principle, this
     * method should be called whenever a change occurs in the logical query plan, but it is very expensive and the shllow version
     * of this method is preferred.
     * 
     * @throws QueryCompilationException
     *             if there is a query compilation error.
     */
    public void updateOutputInfoDeep() throws QueryCompilationException
    {
        m_free_params.clear();
        m_absolute_vars.clear();
        
        // Update assign operators
        for (Assign assign : getAssigns())
        {
            LogicalPlanUtil.updateDescendentOutputInfo(assign);
            m_free_params.addAll(assign.getFreeParametersUsed());
            for (Variable variable : assign.getVariablesUsed())
            {
                if (variable instanceof AbsoluteVariable) m_absolute_vars.add((AbsoluteVariable) variable);
            }
            
        }
        
        LogicalPlanUtil.updateDescendentOutputInfo(m_root);
        
        // Collect the free parameters in the logical plan
        m_free_params.addAll(collectFreeParameters(m_root));
        
        // Collect the absolute variables in the logical plan
        m_absolute_vars.addAll(collectAbsoluteVariables());
        
        // Validate the free parameters
        for (Parameter param : m_free_params)
        {
            param.getTerm().inferType(Collections.<Operator> emptyList());
            param.setType(param.getTerm().getType());
        }
    }
    
    /**
     * Updates the output info of the logical plan without recursively updating the output types of all operators in the logical
     * query plan.
     */
    public void updateOutputInfoShallow()
    {
        // Collect the free parameters in the logical plan
        m_free_params = collectFreeParameters(m_root);
        
        // Collect the absolute variables in the logical plan
        m_absolute_vars = collectAbsoluteVariables();
    }
    
    /**
     * Collects the free parameters in the logical plan.
     * 
     * @param operator
     *            the operator currently being processed.
     * @return a list of all parameters in the logical plan.
     */
    private List<Parameter> collectFreeParameters(Operator operator)
    {
        List<Parameter> free_params = new ArrayList<Parameter>();
        
        for (Operator child : operator.getChildren())
            free_params.addAll(collectFreeParameters(child));
        
        free_params.addAll(operator.getFreeParametersUsed());
        
        return free_params;
    }
    
    /**
     * Collects and returns absolute variables in this logical plan.
     * 
     * @return a list of all absolute variables in the logical plan.
     */
    private List<AbsoluteVariable> collectAbsoluteVariables()
    {
        List<AbsoluteVariable> vars = new ArrayList<AbsoluteVariable>();
        
        for (Operator op : m_root.getDescendantsAndSelf())
            for (Variable var : op.getVariablesUsed())
                if (var instanceof AbsoluteVariable)
                {
                    vars.add((AbsoluteVariable) var);
                }
        
        return vars;
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        return checkSqlCompliance(m_root, metadata);
    }
    
    /**
     * Traverses a logical plan top-down and left-to-right and determines whether all operators are SQL compliant.
     * 
     * @param operator
     *            the root of a logical subtree.
     * @param metadata
     *            the data source to consider.
     * @return true if the logical subtree rooted at the input operator is SQL compliant.
     */
    private boolean checkSqlCompliance(Operator operator, DataSourceMetaData metadata)
    {
        if (!operator.isDataSourceCompliant(metadata)) return false;
        
        for (Operator child : operator.getChildren())
        {
            boolean flag = checkSqlCompliance(child, metadata);
            if (!flag) return false;
        }
        
        return true;
    }
    
    /*
     * The old verbose one.
     * 
     * @Override public String toExplainString() { String class_name = this.getClass().getName();
     * class_name.substring(class_name.lastIndexOf('.') + 1, class_name.length());
     * 
     * StringBuilder sb = new StringBuilder(class_name); sb.append(" - Free Parameters: "); for (Parameter param : m_free_params) {
     * sb.append(param.toExplainString()); sb.append(", "); } if (!m_free_params.isEmpty()) { sb.delete(sb.length() - 2,
     * sb.length()); } sb.append("\n");
     * 
     * sb.append(" - Assign operators:"); for (Assign assign : m_assigns) { sb.append("\n    ");
     * sb.append(assign.toExplainString()); } sb.append("\n");
     * 
     * buildExplainString(m_root, 0, sb);
     * 
     * return sb.toString(); }
     */
    
    @Override
    public String toExplainString()
    {
        // String class_name = this.getClass().getName();
        // class_name.substring(class_name.lastIndexOf('.') + 1, class_name.length());
        
        StringBuilder sb = new StringBuilder();
        
        if (!m_assigns.isEmpty())
        {
            for (Assign assign : m_assigns)
            {
                // sb.append("\n    ");
                buildExplainString(assign, 0, sb);
            }
            sb.append("\n");
        }
        
        buildExplainString(m_root, 0, sb);
        
        return sb.toString();
    }
    
    /**
     * Recursively visits all operators in the logical query plan in a bottom-up fashion.
     * 
     * @param operator
     *            the current operator being visited.
     * @param tabs
     *            the number of indentation tabs.
     * @param sb
     *            the string builder for accumulating explain message.
     */
    public static void buildExplainString(Operator operator, int tabs, StringBuilder sb)
    {
        // for (int i = 0; i < tabs; i++)
        // sb.append("    ");
        // sb.append(operator.getOutputInfo().toExplainString());
        // sb.append("\n");
        
        for (int i = 0; i < tabs; i++)
            sb.append("    ");
        sb.append(operator.toExplainString() + "\n");
        
        for (LogicalPlan logical_plan : operator.getLogicalPlansUsed())
        {
            if (!logical_plan.getAssigns().isEmpty())
            {
                for (Assign assign : logical_plan.getAssigns())
                {
                    buildExplainString(assign, tabs + 2, sb);
                    sb.append("\n");
                }
            }
            buildExplainString(logical_plan.getRootOperator(), tabs + 2, sb);
        }
        
        for (Operator child : operator.getChildren())
        {
            buildExplainString(child, tabs + 1, sb);
        }
    }
    
    @Override
    public String toString()
    {
        return this.toExplainString();
    }
    
    /**
     * Creates a copy of the logical plan that has an up-to-date output info.
     * 
     * @return a copied logical plan.
     */
    public LogicalPlan copy()
    {
        LogicalPlan copy = new LogicalPlan(LogicalPlanUtil.copyDeep(m_root));
        
        // Copy assign
        for (Assign assign : getAssigns())
        {
            copy.addAssignOperator((Assign) assign.copy());
        }
        
        copy.m_is_nested = m_is_nested;
        copy.m_wrapping = this.m_wrapping;
        copy.updateOutputInfoShallow();
        
        return copy;
    }
    
    public void addEquivalentKeys(RelativeVariable var1, RelativeVariable var2)
    {
        assert var1 != null && var2 != null;
        
        // Ignore the variable itself
        if (var1.equals(var2)) return;
        
        KeyGroup g1 = findGroup(var1);
        KeyGroup g2 = findGroup(var2);
        
        if (g1 == null && g2 == null)
        {
            // register a new key group
            KeyGroup g = new KeyGroup();
            g.add(var1);
            g.add(var2);
            m_key_groups.add(g);
        }
        else if (g1 != null && g2 != null)
        {
            assert g1 == g2 : "It's illegeal to merge key groups.";
        }
        else if (g1 != null)
        {
            g1.add(var2);
        }
        else
        {
            g2.add(var1);
        }
    }
    
    /**
     * Checks if the given two relative variables belong to the same equivalent group.
     * 
     * @param var1
     * @param var2
     * @return
     */
    public boolean isEquivalent(RelativeVariable var1, RelativeVariable var2)
    {
        KeyGroup g1 = findGroup(var1);
        KeyGroup g2 = findGroup(var2);
        return g1 != null && g1 == g2;
    }
    
    /**
     * Finds the key group that contains the variable.
     * 
     * @param var
     * @return the key group that contains the variable, null otherwise.
     */
    private KeyGroup findGroup(RelativeVariable var)
    {
        for (KeyGroup group : m_key_groups)
        {
            if (group.contains(var)) return group;
        }
        return null;
    }
    
    public void clearEquivalentGroups()
    {
        m_key_groups.clear();
    }
    
    /**
     * A key-equivalent group.
     * 
     * @author Yupeng Fu
     * 
     */
    private static class KeyGroup extends LinkedHashSet<RelativeVariable>
    {
        private static final long serialVersionUID = 1L;
        
        /**
         * Default constructor. Creates an empty group.
         */
        KeyGroup()
        {
        }
        
    }
    
}
