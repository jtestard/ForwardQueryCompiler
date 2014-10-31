package edu.ucsd.forward.query.logical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.exception.ExceptionMessages.QueryCompilation;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.plan.LogicalPlanUtil;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;
import edu.ucsd.forward.util.NameGenerator;
import edu.ucsd.forward.util.NameGeneratorFactory;

/**
 * Represents the generalized projection logical operator.
 * 
 * @author Michalis Petropoulos
 * @author Romain Vernoux
 */
@SuppressWarnings("serial")
public class Project extends AbstractUnaryOperator
{
    @SuppressWarnings("unused")
    private static final Logger log           = Logger.getLogger(Project.class);
    
    /**
     * Normal form assumption: the project operator always output a tuple with a single attribute.
     */
    public static final String  PROJECT_ALIAS = "__project";
    
    /**
     * The list of attribute names to project.
     */
    private List<String>        m_projection_alias;
    
    /**
     * The list of attribute terms to project.
     */
    private List<Item>          m_projection_item;
    
    /**
     * Tells if the Project operator outputs a tuple (creating a collection of tuples) or a single value (creating a collection of
     * values, possibly scalars).
     */
    public enum ProjectQualifier
    {
        TUPLE, ELEMENT
    }
    
    /**
     * The qualifier for this operator. By default, TUPLE (SQL semantics).
     */
    private ProjectQualifier m_project_qualifier     = ProjectQualifier.TUPLE;
    
    /**
     * The set of relative variables that are used more than once in the projection items.
     */
    private Set<Variable>    m_multi_usage_variables = new LinkedHashSet<Variable>();
    
    /**
     * Create an instance of the operator.
     */
    public Project()
    {
        super();
        
        m_projection_alias = new ArrayList<String>();
        m_projection_item = new ArrayList<Item>();
    }
    
    /**
     * Adds a projection item to the operator.
     * 
     * @param term
     *            the term of the projection item.
     * @param alias
     *            the alias of the projection item.
     * @param unique
     *            determines if the input alias is supposed to be unique. If true, then an exception will be thrown if there is
     *            another projection item with the same alias. If false, and there is another projection item with the same alias,
     *            then a unique suffix will be appended to the input alias.
     * @throws QueryCompilationException
     *             if the input unique flag is set to true and the input alias is not unique.
     */
    public void addProjectionItem(Term term, String alias, boolean unique) throws QueryCompilationException
    {
        if (unique && m_projection_alias.contains(alias))
        {
            throw new QueryCompilationException(QueryCompilation.DUPLICATE_SELECT_ITEM_ALIAS, term.getLocation(), alias);
        }
        
        String unique_alias = alias;
        
        // Accommodate qualified stars that might add duplicate aliases
        while (m_projection_alias.contains(unique_alias))
        {
            // Keep generating unique names until there is no attribute name collision
            unique_alias = alias + NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.VARIABLE_GENERATOR);
        }
        
        m_projection_alias.add(unique_alias);
        m_projection_item.add(new Item(term, unique_alias));
    }
    
    /**
     * Adds a projection item to the operator.
     * 
     * @param term
     *            the term of the projection item.
     * @param alias
     *            the alias of the projection item.
     * @param unique
     *            determines if the input alias is supposed to be unique. If true, then an exception will be thrown if there is
     *            another projection item with the same alias. If false, and there is another projection item with the same alias,
     *            then a unique suffix will be appended to the input alias.
     * @param index
     *            the position of the new term to project in the projection list
     * @throws QueryCompilationException
     *             if the input unique flag is set to true and the input alias is not unique.
     */
    public void addProjectionItem(Term term, String alias, boolean unique, int index) throws QueryCompilationException
    {
        if (unique && m_projection_alias.contains(alias))
        {
            throw new QueryCompilationException(QueryCompilation.DUPLICATE_SELECT_ITEM_ALIAS, term.getLocation(), alias);
        }
        
        String unique_alias = alias;
        
        // Accommodate qualified stars that might add duplicate aliases
        while (m_projection_alias.contains(unique_alias))
        {
            // Keep generating unique names until there is no attribute name collision
            unique_alias = alias + NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.VARIABLE_GENERATOR);
        }
        
        m_projection_alias.add(index, unique_alias);
        m_projection_item.add(index, new Item(term, unique_alias));
    }
    
    /**
     * Removes a projection item from the operator.
     * 
     * @param item
     *            the projection item to remove.
     * @return the (former) index of the removed item
     */
    public int removeProjectionItem(Item item)
    {
        int index = m_projection_item.indexOf(item);
        assert (m_projection_item.remove(item) == true);
        assert (m_projection_alias.remove(item.getAlias()) == true);
        return index;
    }
    
    /**
     * Returns the set of relative variables that are used more than once in the projection items.
     * 
     * @return the set of relative variables that are used more than once in the projection items.
     */
    public Set<Variable> getMultiUsageVariables()
    {
        return m_multi_usage_variables;
    }
    
    /**
     * Returns the projection items of the operator.
     * 
     * @return an iterator over the projection items.
     */
    public List<Item> getProjectionItems()
    {
        return m_projection_item;
    }
    
    /**
     * Returns the projection item of the operator given an alias.
     * 
     * @param alias
     *            a projection item alias.
     * @return a projection item.
     */
    public Item getProjectionItem(String alias)
    {
        int index = m_projection_alias.indexOf(alias);
        return m_projection_item.get(index);
    }
    
    /**
     * Sets the qualifier to specify if this operator comes from SELECT ELEMENT or SELECT (TUPLE).
     * 
     * @param qualifier
     *            the qualifier to set
     */
    public void setProjectQualifier(ProjectQualifier qualifier)
    {
        m_project_qualifier = qualifier;
    }
    
    /**
     * Tells if this operator comes from SELECT ELEMENT or SELECT (TUPLE).
     * 
     * @return the qualifier of this operator
     */
    public ProjectQualifier getProjectQualifier()
    {
        return m_project_qualifier;
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        List<Variable> result = new ArrayList<Variable>();
        
        for (Item projection : m_projection_item)
        {
            result.addAll(projection.getTerm().getVariablesUsed());
        }
        
        return result;
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        List<Parameter> result = new ArrayList<Parameter>();
        
        for (Item projection : m_projection_item)
        {
            result.addAll(projection.getTerm().getFreeParametersUsed());
        }
        
        return result;
    }
    
    @Override
    public List<Parameter> getBoundParametersUsed()
    {
        return Collections.emptyList();
    }
    
    @Override
    public void updateOutputInfo() throws QueryCompilationException
    {
        // Find the multi-usage variables
        m_multi_usage_variables.clear();
        List<RelativeVariable> var_list = LogicalPlanUtil.getRelativeVariables(this.getVariablesUsed());
        int first_index;
        int last_index;
        for (Variable var : var_list)
        {
            first_index = var_list.indexOf(var);
            last_index = var_list.lastIndexOf(var);
            if (first_index != last_index)
            {
                m_multi_usage_variables.add(var);
            }
        }
        
        // Output info
        OutputInfo output_info = new OutputInfo();
        
        // Create the single attribute variable and compute its type
        RelativeVariable new_var = new RelativeVariable(PROJECT_ALIAS);
        new_var.setLocation(new LocationImpl(LocationImpl.UNKNOWN_PATH));
        if (m_project_qualifier == ProjectQualifier.TUPLE)
        {
            TupleType output_type = new TupleType();
            for (int i = 0; i < m_projection_item.size(); i++)
            {
                String project_item_alias = m_projection_alias.get(i);
                Item project_item = m_projection_item.get(i);
                Type project_item_type = project_item.inferOutputType(this);
                output_type.setAttribute(project_item_alias, TypeUtil.cloneNoParent(project_item_type));
            }
            new_var.setType(output_type);
            output_info.add(new_var, new_var);
        }
        else
        {
            // The project can have 0 item at some point of the plan creation
            assert (m_projection_item.size() == 1 || m_projection_item.size() == 0);
            if (m_projection_item.size() == 1)
            {
                Type project_item_type = m_projection_item.get(0).inferOutputType(this);
                new_var.setType(project_item_type);
                output_info.add(new_var, new_var);
            }
        }
        
        // Set the singleton flag of the output info
        output_info.setSingleton(getChild().getOutputInfo().isSingleton());
        
        // Set the output_ordered flag of the output info
        output_info.setOutputOrdered(this.getChild().getOutputInfo().isOutputOrdered());
        
        // Set the actual output info
        this.setOutputInfo(output_info);
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        boolean compliant = true;
        
        switch (metadata.getDataModel())
        {
            case RELATIONAL:
                // Empty projections are not compliant with relational data sources
                if (m_projection_item.size() == 0) compliant = false;
                break;
            default:
                break;
        }
        
        for (Item item : m_projection_item)
        {
            compliant &= item.getTerm().isDataSourceCompliant(metadata);
            if (!compliant) break;
        }
        
        return (compliant && super.isDataSourceCompliant(metadata));
    }
    
    @Override
    public String toExplainString()
    {
        String str = super.toExplainString() + " " + ((m_project_qualifier == ProjectQualifier.ELEMENT) ? "ELEMENT" : "") + "  ";
        for (Item projection : m_projection_item)
        {
            str += projection.getTerm().toExplainString() + " as " + projection.getAlias() + ", ";
        }
        str = str.substring(0, str.length() - 2);
        
        return str;
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitProject(this);
    }
    
    @Override
    public Operator copy()
    {
        Project copy = new Project();
        super.copy(copy);
        
        for (Item item : m_projection_item)
            try
            {
                copy.addProjectionItem(item.m_term.copy(), item.m_alias, false);
            }
            catch (QueryCompilationException e)
            {
                assert (false);
            }
        
        copy.setProjectQualifier(m_project_qualifier);
        
        return copy;
    }
    
    @Override
    public Project copyWithoutType()
    {
        Project copy = new Project();
        
        for (Item item : m_projection_item)
            try
            {
                copy.addProjectionItem(item.m_term.copyWithoutType(), item.m_alias, false);
            }
            catch (QueryCompilationException e)
            {
                assert (false);
            }
        
        copy.setProjectQualifier(m_project_qualifier);
        
        return copy;
    }
    
    /**
     * Represents the generalized projection item.
     * 
     * @author Michalis Petropoulos
     * 
     */
    public static final class Item implements Serializable
    {
        /**
         * The term of the projection item.
         */
        private Term   m_term;
        
        /**
         * The alias of the projection item.
         */
        private String m_alias;
        
        /**
         * The type of the projection item.
         */
        private Type   m_type;
        
        /**
         * Initializes an instance of the projection item.
         * 
         * @param term
         *            the term of the projection item.
         * @param alias
         *            the alias of the projection item.
         */
        private Item(Term term, String alias)
        {
            assert (term != null);
            assert (alias != null);
            
            m_alias = alias;
            m_term = term;
        }
        
        /**
         * private constructor.
         */
        private Item()
        {
            
        }
        
        /**
         * Returns the term of the projection item.
         * 
         * @return the term of the projection item.
         */
        public Term getTerm()
        {
            return m_term;
        }
        
        /**
         * Sets the term of the projection item.
         * 
         * @param term
         *            the term to set
         */
        public void setTerm(Term term)
        {
            assert term != null;
            m_term = term;
        }
        
        /**
         * Returns the alias of the projection item.
         * 
         * @return the alias of the projection item.
         */
        public String getAlias()
        {
            return m_alias;
        }
        
        /**
         * Infers the output type of the projection item.
         * 
         * @param operator
         *            the projection operator to which this item is an argument.
         * @return the output type of the projection item.
         * @throws QueryCompilationException
         *             when encounters type checking error.
         */
        public Type inferOutputType(Project operator) throws QueryCompilationException
        {
            m_type = m_term.inferType(operator.getChildren());
            assert (m_type != null);
            
            return m_type;
        }
        
    }
    
}
