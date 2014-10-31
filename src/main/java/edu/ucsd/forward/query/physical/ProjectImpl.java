package edu.ucsd.forward.query.physical;

import java.util.Collection;

import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.Project;
import edu.ucsd.forward.query.logical.Project.Item;
import edu.ucsd.forward.query.logical.Project.ProjectQualifier;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Implementation of the generalized projection logical operator.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class ProjectImpl extends AbstractUnaryOperatorImpl<Project>
{
    /**
     * The list of projection items.
     */
    private Collection<Item> m_items;
    
    /**
     * Constructs an instance of the physical operator implementation.
     * 
     * @param logical
     *            a logical projection operator.
     * @param child
     *            the single child operator implementation.
     */
    public ProjectImpl(Project logical, OperatorImpl child)
    {
        super(logical, child);
        
        // Cheaper to cache than recompute within next() method.
        m_items = logical.getProjectionItems();
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        // Get the next value from the child operator implementation
        Binding in_binding = this.getChild().next();
        
        if (in_binding == null) return null;
        
        // Reset the cloned flag for query paths used more than once
        BindingValue value;
        for (Variable var : this.getOperator().getMultiUsageVariables())
        {
            value = TermEvaluator.evaluate(var, in_binding);
            value.resetCloned();
        }
        
        // Evaluate the projection items
        Binding out_binding = new Binding();

        
        if(this.getOperator().getProjectQualifier() == ProjectQualifier.ELEMENT)
        {
            assert (m_items.size() == 1);
            BindingValue item_value = TermEvaluator.evaluate(m_items.iterator().next().getTerm(), in_binding);
            out_binding.addValue(item_value);
        }
        else
        {
            // Wrap the the projection items inside in a tuple value
            TupleValue tuple_value = new TupleValue();
            
            for (Item item : m_items)
            {
                Value item_value = TermEvaluator.evaluate(item.getTerm(), in_binding).getValue();
                tuple_value.setAttribute(item.getAlias(), ValueUtil.cloneNoParentNoType(item_value));
            }
            
            out_binding.addValue(new BindingValue(tuple_value, true));
        }
        
        return out_binding;
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        ProjectImpl copy = new ProjectImpl(getOperator(), getChild().copy(context));
        
        copy.m_items = m_items;
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitProjectImpl(this);
    }
    
}
