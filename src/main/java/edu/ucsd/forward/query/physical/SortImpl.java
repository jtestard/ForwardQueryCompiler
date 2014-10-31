package edu.ucsd.forward.query.physical;

import java.util.PriorityQueue;

import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.Sort;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Implementation of the sort logical operator. This implementation uses a priority queue to sort: insert all the bindings to be
 * sorted into a priority queue, and sequentially remove them, which both are O(log(n)) time. The advantage of this approach is the
 * performance gain when the sort operator used together with fetch offset operator, as it won't fully sort all the bindings but the
 * top K.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 */
public class SortImpl extends AbstractUnaryOperatorImpl<Sort>
{
    private static final long              serialVersionUID = 1L;
    
    /**
     * The initial capacity to create the priority queue, which is required by the constructor of Java class.
     */
    private static final int               INITIAL_CAPACITY = 10;
    
    private PriorityQueue<SortableBinding> m_priority_queue;
    
    private boolean                        m_init           = false;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical selection operator.
     * @param child
     *            the single child operator implementation.
     */
    public SortImpl(Sort logical, OperatorImpl child)
    {
        super(logical, child);
        m_priority_queue = new PriorityQueue<SortableBinding>(INITIAL_CAPACITY,
                                                              new BindingComparator(this.getOperator().getSortItems()));
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        m_priority_queue.clear();
        m_init = false;
        super.open();
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        if (!m_init)
        {
            // Enqueue the bindings
            SortableBinding s_binding;
            Binding in_binding = getChild().next();
            while (in_binding != null)
            {
                s_binding = new SortableBinding(in_binding, this.getOperator().getSortItems());
                m_priority_queue.add(s_binding);
                
                in_binding = getChild().next();
            }
            m_init = true;
        }
        
        if (m_priority_queue.isEmpty())
        {
            return null;
        }
        
        // Dequeue
        return m_priority_queue.poll().getBinding();
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        if (this.getState() == State.OPEN)
        {
            m_priority_queue.clear();
            m_init = false;
        }
        
        super.close();
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        SortImpl copy = new SortImpl(this.getOperator(), this.getChild().copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitSortImpl(this);
    }
}
