/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.aggregate.AggregateFunctionCall;
import edu.ucsd.forward.query.function.aggregate.NestFunction;
import edu.ucsd.forward.query.logical.GroupBy;
import edu.ucsd.forward.query.logical.GroupBy.Aggregate;
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Implementation of the group by logical operator.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 * 
 */
@SuppressWarnings("serial")
public class GroupByImpl extends AbstractUnaryOperatorImpl<GroupBy>
{
    @SuppressWarnings("unused")
    private static final Logger                           log = Logger.getLogger(GroupByImpl.class);
    
    /**
     * The buffer for the group by terms.
     */
    private BindingBufferHash                             m_groups;
    
    /**
     * The current group.
     */
    private Iterator<Entry<Binding, Collection<Binding>>> m_current_group;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical selection operator.
     * @param child
     *            the single child operator implementation.
     */
    public GroupByImpl(GroupBy logical, OperatorImpl child)
    {
        super(logical);
        
        assert (child != null);
        
        this.addChild(child);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        m_current_group = null;
        
        // The group by terms will participate in the index key
        OutputInfo in_info = this.getOperator().getChild().getOutputInfo();
        
        // Form the conditions to be used by the binding buffer
        List<Term> conditions = new ArrayList<Term>();
        for (Term term : this.getOperator().getGroupByTerms())
        {
            conditions.add(term);
        }
        
        m_groups = new BindingBufferHash(conditions, in_info, false);
        
        super.open();
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        // Construct groups
        if (m_current_group == null)
        {
            m_groups.populate(getChild());
            m_current_group = m_groups.getBindingBuckets();
            
            // Create an empty collection if input is empty and there are no group by terms
            if (m_groups.isEmpty() && this.getOperator().getGroupByTerms().isEmpty())
            {
                Map<Binding, Collection<Binding>> empty = new HashMap<Binding, Collection<Binding>>();
                empty.put(new Binding(), Collections.<Binding> emptyList());
                m_current_group = empty.entrySet().iterator();
            }
        }
        
        if (m_current_group.hasNext())
        {
            Entry<Binding, Collection<Binding>> bucket = m_current_group.next();
            // Construct output binding
            Binding out_binding = new Binding();
            out_binding.addValues(bucket.getKey().getValues());
            
            for (Term term : this.getOperator().getCarryOnTerms())
            {
                Collection<Binding> value = bucket.getValue();
                assert (!value.isEmpty()); // the group shall not be empty if it exists.
                
                Binding first_item = value.iterator().next();
                
                BindingValue carry_on = TermEvaluator.evaluate(term, first_item);
                if (carry_on != null)
                {
                    out_binding.addValue(carry_on);
                }
            }
            
            // Compute the aggregates
            for (Aggregate aggregate : getOperator().getAggregates())
            {
                AggregateFunctionCall call = aggregate.getAggregateFunctionCall();
                if (!(call.getFunction() instanceof NestFunction) && call.getFunctionSignature() == null)
                {
                    if (bucket.getValue().isEmpty())
                    {
                        throw new QueryExecutionException(QueryExecution.AGGREGATE_ON_JAVA_WITH_EMPTY_COLLECTION,
                                                          call.getFunction().getName());
                    }
                    call.matchFunctionSignature(new ArrayList<Binding>(bucket.getValue()));
                }
                out_binding.addValue(call.getFunction().evaluate(call, bucket.getValue(), call.getSetQuantifier()));
            }
            
            return out_binding;
        }
        // There are no groups
        else
        {
            return null;
        }
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        if (this.getState() == State.OPEN)
        {
            m_groups.clear();
            m_current_group = null;
        }
        
        super.close();
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        GroupByImpl copy = new GroupByImpl(this.getOperator(), this.getChild().copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitGroupByImpl(this);
    }
}
