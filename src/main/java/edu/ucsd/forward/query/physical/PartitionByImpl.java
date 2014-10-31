/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.PartitionBy;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * The implementation of the partition by operator.
 * 
 * @author Yupeng
 * 
 */
public class PartitionByImpl extends AbstractUnaryOperatorImpl<PartitionBy>
{
    private static final long                             serialVersionUID = 1L;
    
    @SuppressWarnings("unused")
    private static final Logger                           log              = Logger.getLogger(PartitionByImpl.class);
    
    /**
     * The buffer for the partition by terms.
     */
    private BindingBufferHash                             m_partitions;
    
    /**
     * The initial capacity to create the priority queue, which is required by the constructor of Java class.
     */
    private static final int                              INITIAL_CAPACITY = 10;
    
    private int                                           m_fetch;
    
    /**
     * The rank of the tuples in each partition.
     */
    private int                                           m_rank;
    
    /**
     * The current partition.
     */
    private Iterator<Entry<Binding, Collection<Binding>>> m_current_partition;
    
    private PriorityQueue<SortableBinding>                m_priority_queue;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical selection operator.
     * @param child
     *            the single child operator implementation.
     */
    public PartitionByImpl(PartitionBy logical, OperatorImpl child)
    {
        super(logical);
        
        assert (child != null);
        
        this.addChild(child);
        
        int capacity = (getOperator().getLimit() == null) ? INITIAL_CAPACITY : getOperator().getLimit().getObject();
        m_priority_queue = new PriorityQueue<SortableBinding>(capacity, new BindingComparator(this.getOperator().getSortByItems()));
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitPartitionByImpl(this);
        
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        PartitionByImpl copy = new PartitionByImpl(getOperator(), getChild().copy(context));
        
        return copy;
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        m_current_partition = null;
        
        // The p by terms will participate in the index key
        OutputInfo in_info = this.getOperator().getChild().getOutputInfo();
        
        // Form the conditions to be used by the binding buffer
        List<Term> conditions = new ArrayList<Term>();
        for (Term term : this.getOperator().getPartitionByTerms())
        {
            conditions.add(term);
        }
        
        m_partitions = new BindingBufferHash(conditions, in_info, false);
        
        m_fetch = Integer.MIN_VALUE;
        
        m_priority_queue.clear();
        
        m_rank = 0;
        
        super.open();
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        if (this.getState() == State.OPEN)
        {
            m_partitions.clear();
            m_current_partition = null;
            m_priority_queue.clear();
        }
        
        super.close();
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        // Construct partitions
        if (m_current_partition == null)
        {
            m_partitions.populate(getChild());
            m_current_partition = m_partitions.getBindingBuckets();
        }
        
        if (!hasNextInCurrentPartition() && m_current_partition.hasNext())
        {
            // Load the next partition
            Entry<Binding, Collection<Binding>> bucket = m_current_partition.next();
            
            // Load tuples in a partition
            SortableBinding s_binding;
            m_priority_queue.clear();
            for (Binding binding : bucket.getValue())
            {
                s_binding = new SortableBinding(binding, this.getOperator().getSortByItems());
                m_priority_queue.add(s_binding);
            }
            
            // Reset the limit
            if (getOperator().getLimit() != null) m_fetch = getOperator().getLimit().getObject();
            
            // Reset the rank
            if (getOperator().hasRankFunction()) m_rank = 1;
        }
        
        if (hasNextInCurrentPartition())
        {
            Binding out_binding = m_priority_queue.poll().getBinding();
            if (m_fetch != Integer.MIN_VALUE) m_fetch--;
            if (getOperator().hasRankFunction())
            {
                out_binding.addValue(new BindingValue(new IntegerValue(m_rank), false));
                m_rank++;
            }
            return out_binding;
        }
        
        // There is no partition left
        return null;
    }
    
    /**
     * Checks whether there is next binding available in the current binding.
     * 
     * @return whether there is next binding available in the current binding.
     */
    private boolean hasNextInCurrentPartition()
    {
        if (m_priority_queue.isEmpty()) return false;
        
        if (m_fetch == 0) return false;
        
        return true;
    }
    
}
