package edu.ucsd.forward.query.logical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.OrderByItem.Nulls;
import edu.ucsd.forward.query.ast.OrderByItem.Spec;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

/**
 * Represents the sort logical operator.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class Sort extends AbstractUnaryOperator
{
    /**
     * The list of sort items.
     */
    private List<Item> m_items;
    
    /**
     * Initializes an instance of the operator.
     */
    public Sort()
    {
        m_items = new ArrayList<Item>();
    }
    
    /**
     * Adds a sort item to the operator.
     * 
     * @param term
     *            the term of the sort item.
     * @param spec
     *            the sorting specification of the sort item.
     * @param nulls
     *            the nulls ordering of the sort item.
     */
    public void addSortItem(Term term, Spec spec, Nulls nulls)
    {
        m_items.add(new Item(term, spec, nulls));
    }
    
    /**
     * Removes a sort item from the operator.
     * 
     * @param sort
     *            the sort item to remove.
     */
    public void removeSortItem(Item sort)
    {
        assert (m_items.remove(sort));
    }
    
    /**
     * Returns the sort items of the operator.
     * 
     * @return a list of sort items.
     */
    public List<Item> getSortItems()
    {
        return m_items;
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        List<Variable> result = new ArrayList<Variable>();
        
        for (Item sort : m_items)
        {
            result.addAll(sort.getTerm().getVariablesUsed());
        }
        
        return result;
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        List<Parameter> result = new ArrayList<Parameter>();
        
        for (Item sort : m_items)
        {
            result.addAll(sort.getTerm().getFreeParametersUsed());
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
        for (Item item : m_items)
        {
            item.getTerm().inferType(this.getChildren());
        }
        
        // Output info
        OutputInfo output_info = new OutputInfo();
        
        // Copy the input variables and keys
        for (RelativeVariable var : this.getInputVariables())
        {
            output_info.add(var, var);
        }
        output_info.setKeyTerms(this.getChild().getOutputInfo().getKeyTerms());
        
        // Set the output_ordered flag of the output info
        output_info.setOutputOrdered(true);
        
        // Set the actual output info
        this.setOutputInfo(output_info);
    }
    
    @Override
    public String toExplainString()
    {
        String str = super.toExplainString() + " ";
        for (Item sort : m_items)
        {
            str += sort.getTerm().toExplainString() + " " + sort.getSpec().name() + " " + sort.getNulls().name() + ", ";
        }
        str = str.substring(0, str.length() - 2);
        
        return str;
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitSort(this);
    }
    
    @Override
    public Operator copy()
    {
        Sort copy = new Sort();
        super.copy(copy);
        
        for (Item item : m_items)
            copy.addSortItem(item.m_term.copy(), item.m_spec, item.m_nulls);
        
        return copy;
    }
    
    /**
     * Represents the sort item.
     * 
     * @author Michalis Petropoulos
     * 
     */
    public final class Item
    {
        /**
         * The term of the sort item.
         */
        private Term  m_term;
        
        private Spec  m_spec;
        
        private Nulls m_nulls;
        
        /**
         * Initializes an instance of the sort item.
         * 
         * @param term
         *            the term of the sort item.
         * @param spec
         *            the sorting specification of the sort item.
         * @param nulls
         *            the nulls ordering of the sort item.
         */
        private Item(Term term, Spec spec, Nulls nulls)
        {
            assert (term != null);
            assert (spec != null);
            assert (nulls != null);
            
            m_term = term;
            m_spec = spec;
            m_nulls = nulls;
        }
        
        /**
         * Returns the term of the sort item.
         * 
         * @return the term of the sort item.
         */
        public Term getTerm()
        {
            return m_term;
        }
        
        public void setTerm(Term term)
        {
            m_term = term;
        }
        
        /**
         * Returns the ordering specification of the sort item.
         * 
         * @return the ordering specification of the sort item.
         */
        public Spec getSpec()
        {
            return m_spec;
        }
        
        /**
         * Returns the nulls ordering of the sort item.
         * 
         * @return the nulls ordering of the sort item.
         */
        public Nulls getNulls()
        {
            return m_nulls;
        }
        
    }
    
}
