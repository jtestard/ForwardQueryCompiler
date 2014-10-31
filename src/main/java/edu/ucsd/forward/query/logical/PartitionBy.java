/**
 * 
 */
package edu.ucsd.forward.query.logical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.Sort.Item;
import edu.ucsd.forward.query.logical.term.Constant;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.KeyInferenceBuilder;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

/**
 * Represents a Partition operator that groups tuples by a set of terms but unlike the GroupBy operator, retains all the tuples that
 * belong to a group.
 * 
 * @author <Vicky Papavasileiou>
 * 
 */
public class PartitionBy extends AbstractUnaryOperator
{
    
    private static final long      serialVersionUID = 1L;
    
    @SuppressWarnings("unused")
    private static final Logger    log              = Logger.getLogger(PartitionBy.class);
    
    /**
     * Partition by terms.
     */
    private List<RelativeVariable> m_partition_by_terms;
    
    /**
     * Optional list of sort items.
     */
    private List<Item>             m_sort_by_items;
    
    /**
     * Optional number of limit for size of partition.
     */
    private IntegerValue           m_limit;
    
    /**
     * Optional alias of the rank function.
     */
    private String                 m_rank_alias;
    
    public PartitionBy()
    {
        m_partition_by_terms = new ArrayList<RelativeVariable>();
        m_sort_by_items = new ArrayList<Item>();
    }
    
    /**
     * @return the partition_by_terms
     */
    public List<RelativeVariable> getPartitionByTerms()
    {
        return m_partition_by_terms;
    }
    
    /**
     * @param partitionByTerms
     *            the partition_by_terms to set
     */
    public void setPartitionByTerms(List<RelativeVariable> partitionByTerms)
    {
        m_partition_by_terms = partitionByTerms;
    }
    
    public void addPartitionByTerm(RelativeVariable term)
    {
        m_partition_by_terms.add(term);
    }
    
    public void removePartitionByTerm(RelativeVariable term)
    {
        m_partition_by_terms.remove(term);
    }
    
    /**
     * @return the items
     */
    public List<Item> getSortByItems()
    {
        return m_sort_by_items;
    }
    
    /**
     * @param items
     *            the items to set
     */
    public void setSortByItems(List<Item> items)
    {
        m_sort_by_items = items;
    }
    
    public void addSortByItem(Item item)
    {
        m_sort_by_items.add(item);
    }
    
    /**
     * @return the limit
     */
    public IntegerValue getLimit()
    {
        return m_limit;
    }
    
    /**
     * @param limit
     *            the limit to set
     */
    public void setLimit(IntegerValue limit)
    {
        m_limit = limit;
    }
    
    /**
     * Gets the alias of the rank function.
     * 
     * @return the alias of the rank function.
     */
    public String getRankAlias()
    {
        return m_rank_alias;
    }
    
    /**
     * Sets the rank alias.
     * 
     * @param alias
     *            the rank alias.
     */
    public void setRankAlias(String alias)
    {
        m_rank_alias = alias;
    }
    
    /**
     * Returns whether the operator has rank function.
     * 
     * @return whether the operator has rank function.
     */
    public boolean hasRankFunction()
    {
        return m_rank_alias != null;
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitPartitionBy(this);
    }
    
    @Override
    public Operator copy()
    {
        PartitionBy copy = new PartitionBy();
        super.copy(copy);
        
        for (RelativeVariable var : m_partition_by_terms)
            copy.addPartitionByTerm((RelativeVariable) var.copy());
        
        for (Item item : m_sort_by_items)
            copy.addSortByItem(item);
        
        copy.setLimit(m_limit);
        
        copy.setRankAlias(m_rank_alias);
        
        return copy;
    }
    
    @Override
    public void updateOutputInfo() throws QueryCompilationException
    {
        // Output info
        OutputInfo output_info = new OutputInfo();
        
        // Infer the type of the partition by variables
        for (RelativeVariable var : m_partition_by_terms)
        {
            var.inferType(this.getChildren());
        }
        
        // Infer the type of sort by variables
        for (Item sort_item : m_sort_by_items)
        {
            sort_item.getTerm().inferType(this.getChildren());
        }
        
        // Copy the input variables and keys
        for (RelativeVariable var : this.getInputVariables())
        {
            // Ignore the retouched attributes, when the rank alias is present
            if (hasRankFunction() && KeyInferenceBuilder.isRetouchedVariable(var.getName())) continue;
            output_info.add(var, var);
        }
        
        // Add the rank attribute
        if (hasRankFunction())
        {
            // Use a constant to denote the rank attribute
            Constant rank = new Constant(new IntegerValue(1));
            Type output_type = rank.inferType(Collections.<Operator> singletonList(this));
            
            // Add output info entry
            RelativeVariable rank_var = new RelativeVariable(getRankAlias());
            rank_var.setType(output_type);
            output_info.add(rank_var, rank);
        }
        
        output_info.setKeyTerms(this.getChild().getOutputInfo().getKeyTerms());
        
        // Set the actual output info
        this.setOutputInfo(output_info);
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        List<Variable> result = new ArrayList<Variable>();
        
        for (Term term : m_partition_by_terms)
        {
            result.addAll(term.getVariablesUsed());
        }
        
        for (Item sort_item : m_sort_by_items)
        {
            result.addAll(sort_item.getTerm().getVariablesUsed());
        }
        
        return result;
    }
    
    @Override
    public List<Parameter> getBoundParametersUsed()
    {
        return Collections.emptyList();
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        List<Parameter> result = new ArrayList<Parameter>();
        
        for (Term term : m_partition_by_terms)
        {
            result.addAll(term.getFreeParametersUsed());
        }
        
        for (Item sort_item : m_sort_by_items)
        {
            result.addAll(sort_item.getTerm().getFreeParametersUsed());
        }
        
        return result;
    }
    
    @Override
    public String toExplainString()
    {
        String str = super.toExplainString() + " ";
        
        str += "BY: ";
        for (Term term : getPartitionByTerms())
        {
            str += term.toExplainString() + ", ";
        }
        if (!getPartitionByTerms().isEmpty()) str = str.substring(0, str.length() - 2);
        
        str += " ; ";
        
        if(!getSortByItems().isEmpty()) 
        {
            str+=" ORDER BY: ";
           for(Sort.Item item:getSortByItems())
           {
               str+=item.toString()+", ";
           }
        }
        
        return str;
    }
}
