/**
 * 
 */
package edu.ucsd.forward.query.logical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.function.aggregate.AggregateFunctionCall;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;
import edu.ucsd.forward.util.NameGenerator;
import edu.ucsd.forward.util.NameGeneratorFactory;

/**
 * Represents the group by logical operator.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class GroupBy extends AbstractUnaryOperator
{
    @SuppressWarnings("unused")
    private static final Logger log          = Logger.getLogger(GroupBy.class);
    
    public static final String  CARRY_ON_TAG = "CarryOnItem";
    
    /**
     * The grouping items.
     */
    private List<Item>          m_group_by_items;
    
    /**
     * Carry-on items are functionally dependent on the grouping items. They are unique within each group but don't participate in
     * the grouping process. See Query Processor specification for more details.
     */
    private List<Item>          m_carry_on_items;
    
    /**
     * The aggregates.
     */
    private List<Aggregate>     m_aggregates;
    
    /**
     * Initializes an instance of the operator.
     */
    public GroupBy()
    {
        super();
        
        m_group_by_items = new ArrayList<Item>();
        m_carry_on_items = new ArrayList<Item>();
        m_aggregates = new ArrayList<Aggregate>();
    }
    
    /**
     * Returns the group by terms of the operator.
     * 
     * @return a list of group by terms.
     */
    public List<Term> getGroupByTerms()
    {
        List<Term> terms = new ArrayList<Term>(m_group_by_items.size());
        for (Item item : m_group_by_items)
        {
            terms.add(item.getTerm());
        }
        return terms;
    }
    
    /**
     * Returns the group by items of the operator.
     * 
     * @return a list of group by items.
     */
    public List<Item> getGroupByItems()
    {
        return m_group_by_items;
    }
    
    /**
     * Adds a group by term to the operator.
     * 
     * @param term
     *            the group by term to add.
     */
    public void addGroupByTerm(Term term)
    {
        if (term instanceof RelativeVariable)
        {
            addGroupByTerm(term, ((RelativeVariable) term).getName());
        }
        else
        {
            String alias = NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.VARIABLE_GENERATOR);
            addGroupByTerm(term, alias);
        }
    }
    
    /**
     * Adds a group by term to the operator.
     * 
     * @param term
     *            the group by term to add.
     * @param alias
     *            the alias of the term.
     */
    public void addGroupByTerm(Term term, String alias)
    {
        m_group_by_items.add(new Item(term, alias));
    }
    
    /**
     * Removes a group by term from the operator.
     * 
     * @param term
     *            the group by term to remove.
     */
    public void removeGroupByTerm(Term term)
    {
        for (Item item : m_group_by_items)
        {
            if (item.getTerm().equals(term))
            {
                m_group_by_items.remove(item);
                break;
            }
        }
    }
    
    /**
     * Returns the carry-on terms of the operator.
     * 
     * @return a list of carry-on terms.
     */
    public List<Term> getCarryOnTerms()
    {
        List<Term> terms = new ArrayList<Term>(m_carry_on_items.size());
        for (Item item : m_carry_on_items)
        {
            terms.add(item.getTerm());
        }
        return terms;
    }
    
    /**
     * Returns the carry-on items of the operator.
     * 
     * @return a list of carry-on items.
     */
    public List<Item> getCarryOnItems()
    {
        return m_carry_on_items;
    }
    
    /**
     * Adds a carry-on term to the operator.
     * 
     * @param term
     *            the carry-on term to add.
     */
    public void addCarryOnTerm(Term term)
    {
        if (term instanceof RelativeVariable)
        {
            addCarryOnTerm(term, ((RelativeVariable) term).getName());
        }
        else
        {
            String alias = NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.VARIABLE_GENERATOR);
            addCarryOnTerm(term, alias);
        }
    }
    
    /**
     * Adds a carry-on term to the operator.
     * 
     * @param term
     *            the carry-on to add.
     * @param alias
     *            the alias of the term.
     */
    public void addCarryOnTerm(Term term, String alias)
    {
        m_carry_on_items.add(new Item(term, alias));
    }
    
    /**
     * Removes a carry-on term from the operator.
     * 
     * @param term
     *            the group by term to add.
     */
    public void removeCarryOnTerm(Term term)
    {
        for (Item item : m_carry_on_items)
        {
            if (item.getTerm().equals(term))
            {
                m_carry_on_items.remove(item);
                break;
            }
        }
    }
    
    /**
     * Returns the aggregates of the operator.
     * 
     * @return a list of aggregates.
     */
    public List<Aggregate> getAggregates()
    {
        return m_aggregates;
    }
    
    /**
     * Returns the aggregate by the specified alias.
     * 
     * @param alias
     *            the specified alias
     * @return the aggregate that has the specified alias
     */
    public Aggregate getAggregate(String alias)
    {
        for (Aggregate aggregate : m_aggregates)
        {
            if (aggregate.getAlias().equals(alias)) return aggregate;
        }
        return null;
    }
    
    /**
     * Adds an aggregate to the operator.
     * 
     * @param call
     *            the function call of the aggregate to add.
     * @param alias
     *            the alias of the aggregate to add.
     */
    public void addAggregate(AggregateFunctionCall call, String alias)
    {
        m_aggregates.add(new Aggregate(call, alias));
    }
    
    /**
     * Removes an aggregate from the operator.
     * 
     * @param aggregate
     *            the aggregate to remove.
     */
    public void removeAggregate(Aggregate aggregate)
    {
        assert (this.m_aggregates.remove(aggregate));
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        List<Variable> result = new ArrayList<Variable>();
        
        for (Term term : getGroupByTerms())
        {
            result.addAll(term.getVariablesUsed());
        }
        
        for (Term term : getCarryOnTerms())
        {
            result.addAll(term.getVariablesUsed());
        }
        
        for (Aggregate aggregate : m_aggregates)
        {
            result.addAll(aggregate.getAggregateFunctionCall().getVariablesUsed());
        }
        
        return result;
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        List<Parameter> result = new ArrayList<Parameter>();
        
        for (Term term : getGroupByTerms())
        {
            result.addAll(term.getFreeParametersUsed());
        }
        
        for (Term term : getCarryOnTerms())
        {
            result.addAll(term.getFreeParametersUsed());
        }
        
        for (Aggregate aggregate : m_aggregates)
        {
            result.addAll(aggregate.getAggregateFunctionCall().getFreeParametersUsed());
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
        // Output info
        OutputInfo output_info = new OutputInfo();
        
        // Add the group by variables
        for (Item item : m_group_by_items)
        {
            Term term = item.getTerm();
            Type term_type = term.inferType(this.getChildren());
            
            RelativeVariable rel_var = item.getVariable();
            rel_var.setType(term_type);
            rel_var.setLocation(term.getLocation());
            output_info.add(rel_var, term);
        }
        
        // Add the group by variables
        for (Item item : m_carry_on_items)
        {
            Term term = item.getTerm();
            Type term_type = term.inferType(this.getChildren());
            
            RelativeVariable rel_var = item.getVariable();
            rel_var.setType(term_type);
            rel_var.setLocation(term.getLocation());
            output_info.add(rel_var, term);
        }
        
        // Add aggregates
        for (Aggregate aggregate : m_aggregates)
        {
            // Add attribute to output type
            Type output_type = aggregate.getAggregateFunctionCall().inferType(Collections.<Operator> singletonList(this.getChild()));
            
            // Add output info entry
            RelativeVariable agg_var = new RelativeVariable(aggregate.getAlias());
            agg_var.setType(output_type);
            output_info.add(agg_var, aggregate.getAggregateFunctionCall());
        }
        
        // Set the actual output info
        this.setOutputInfo(output_info);
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        boolean compliant = true;
        
        // If the operator contains carry-on items, it is not compliant.
        if (!m_carry_on_items.isEmpty()) return false;
        
        for (Term term : getGroupByTerms())
        {
            compliant &= term.isDataSourceCompliant(metadata);
            if (!compliant) return false;
        }
        
        for (Aggregate agg : m_aggregates)
        {
            compliant &= agg.getAggregateFunctionCall().isDataSourceCompliant(metadata);
            if (!compliant) return false;
        }
        
        return (super.isDataSourceCompliant(metadata));
    }
    
    @Override
    public String toExplainString()
    {
        String str = super.toExplainString() + " ";
        
        str += "BY: ";
        for (Item item : m_group_by_items)
        {
            str += item.getTerm().toExplainString() + " -> " + item.getVariable() + ", ";
        }
        if (!m_group_by_items.isEmpty())
        {
            str = str.substring(0, str.length() - 2);
        }
        str += " ; ";
        if (m_group_by_items.isEmpty())
        {
            str = str.substring(0, str.length() - 7);
        }
        
        str += "CARRY: ";
        for (Item item : m_carry_on_items)
        {
            str += item.getTerm().toExplainString() + " -> " + item.getVariable() + ", ";
        }
        if (!m_carry_on_items.isEmpty())
        {
            str = str.substring(0, str.length() - 2);
        }
        str += " ; ";
        if (m_carry_on_items.isEmpty())
        {
            str = str.substring(0, str.length() - 10);
        }
        
        str += "AGG: ";
        for (Aggregate aggregate : m_aggregates)
        {
            str += aggregate.getAggregateFunctionCall().toExplainString() + " AS " + aggregate.getAlias() + ", ";
        }
        if (!m_aggregates.isEmpty())
        {
            str = str.substring(0, str.length() - 2);
        }
        if (m_aggregates.isEmpty())
        {
            str = str.substring(0, str.length() - 5);
        }
        
        return str;
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitGroupBy(this);
    }
    
    @Override
    public Operator copy()
    {
        GroupBy copy = new GroupBy();
        super.copy(copy);
        
        for (Item item : m_group_by_items)
        {
            copy.addGroupByTerm(item.getTerm().copy(), item.getVariable().getName());
        }
        
        for (Item item : m_carry_on_items)
        {
            copy.addCarryOnTerm(item.getTerm().copy(), item.getVariable().getName());
        }
        
        for (Aggregate agg : m_aggregates)
            copy.addAggregate((AggregateFunctionCall) agg.m_call.copy(), agg.m_alias);
        
        return copy;
    }
    
    @Override
    public GroupBy copyWithoutType()
    {
        GroupBy copy = new GroupBy();
        
        for (Item item : m_group_by_items)
        {
            copy.addGroupByTerm(item.getTerm().copyWithoutType(), item.getVariable().getName());
        }
        
        for (Item item : m_carry_on_items)
        {
            copy.addCarryOnTerm(item.getTerm().copyWithoutType(), item.getVariable().getName());
        }
        
        for (Aggregate agg : m_aggregates)
            copy.addAggregate(agg.m_call.copyWithoutType(), agg.m_alias);
        
        return copy;
    }
    
    /**
     * Represents a group by item.
     * 
     * @author Michalis Petropoulos
     * 
     */
    public static final class Item implements Serializable
    {
        /**
         * The term of the item.
         */
        private Term             m_term;
        
        /**
         * The variable for the term.
         */
        private RelativeVariable m_alias_var;
        
        /**
         * Initializes an instance of an item.
         * 
         * @param term
         *            the term of the item.
         * @param alias
         *            the alias of the item
         */
        private Item(Term term, String alias)
        {
            assert (term != null);
            assert (alias != null);
            
            m_alias_var = new RelativeVariable(alias);
            m_term = term;
        }
        
        /**
         * Private constructor.
         */
        private Item()
        {
            
        }
        
        /**
         * Returns the term of the item.
         * 
         * @return the term of the item
         */
        public Term getTerm()
        {
            return m_term;
        }
        
        /**
         * Sets the term of the item.
         * 
         * @param term
         *            the term to set
         */
        public void setTerm(Term term)
        {
            assert (term != null);
            m_term = term;
        }
        
        /**
         * Returns the alias variable of the item.
         * 
         * @return the alias variable of the item
         */
        public RelativeVariable getVariable()
        {
            return m_alias_var;
        }
    }
    
    /**
     * Represents an aggregate.
     * 
     * @author Michalis Petropoulos
     * 
     */
    public static final class Aggregate implements Serializable
    {
        /**
         * The aggregate function call of the aggregate.
         */
        private AggregateFunctionCall m_call;
        
        /**
         * The alias of the aggregate.
         */
        private String                m_alias;
        
        /**
         * Initializes an instance of an aggregate.
         * 
         * @param call
         *            the aggregate function call of the aggregate.
         * @param alias
         *            the alias of the aggregate.
         */
        private Aggregate(AggregateFunctionCall call, String alias)
        {
            assert (call != null);
            assert (alias != null);
            
            m_alias = alias;
            m_call = call;
        }
        
        /**
         * Private constructor.
         */
        private Aggregate()
        {
            
        }
        
        /**
         * Returns the aggregate function call of the aggregate.
         * 
         * @return an aggregate function call.
         */
        public AggregateFunctionCall getAggregateFunctionCall()
        {
            return m_call;
        }
        
        /**
         * Sets the function call of the item.
         * 
         * @param call
         *            the call to set
         */
        public void setAggregateFunctionCall(AggregateFunctionCall call)
        {
            assert (call != null);
            m_call = call;
        }
        
        /**
         * Returns the alias of the aggregate.
         * 
         * @return the alias of the aggregate.
         */
        public String getAlias()
        {
            return m_alias;
        }
    }
    
}
