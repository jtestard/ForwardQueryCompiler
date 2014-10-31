/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.value.LongValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.LazyQueryResult;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.Project;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * The implementation of the Assign operator.
 * 
 * @author Yupeng Fu
 * 
 */
public class AssignMemoryToMemoryImpl extends AbstractAssignImpl
{
    private static final long                 serialVersionUID = 1L;
    
    @SuppressWarnings("unused")
    private static final Logger               log              = Logger.getLogger(AssignMemoryToMemoryImpl.class);
    
    private PhysicalPlan                      m_assign_plan;
    
    /**
     * The tuple value iterator.
     */
    private LinkedList<TupleValue>            m_tuple_list;
    
    /**
     * The query result of evaluating the assign plan.
     */
    private LazyQueryResult                   m_result;
    
    private Map<ScanImplReferencing, Integer> m_cursors;
    
    /**
     * The counter for the position variable.
     */
    private long                              m_order_counter;
    
    /**
     * Constructor.
     * 
     * @param assign
     *            the logical assign operator.
     */
    public AssignMemoryToMemoryImpl(Assign assign)
    {
        super(assign);
        m_cursors = new HashMap<ScanImplReferencing, Integer>();
    }
    
    /**
     * Sets the assign plan.
     * 
     * @param plan
     *            the plan to assign.
     */
    public void setAssignPlan(PhysicalPlan plan)
    {
        assert plan != null;
        m_assign_plan = plan;
    }
    
    /**
     * Adds a reference from a referencing scan impl.
     * 
     * @param reference
     *            the referencing scan impl.
     */
    protected void addReference(ScanImplReferencing reference)
    {
        assert reference != null;
        m_cursors.put(reference, 0);
    }
    
    /**
     * Gets the next binding for the referencing scan operator.
     * 
     * @param reference
     *            the referencing scan operator.
     * @return the next binding that the referencing scan operator requests.
     * @throws QueryExecutionException
     *             if anything goes wrong calling assign's next method.
     * @throws SuspensionException
     *             when the execution suspenses
     */
    protected Binding getNext(ScanImplReferencing reference) throws QueryExecutionException, SuspensionException
    {
        assert reference != null;
        assert m_cursors.containsKey(reference);
        int cursor = m_cursors.get(reference);
        
        Binding binding = new Binding();
        
        TupleValue tuple = null;
        
        if (cursor >= m_tuple_list.size())
        {
            // Try fetch one more tuple from assign impl.
            next();
        }
        
        if (cursor >= m_tuple_list.size())
        {
            // The tuple is exhausted from input
            return null;
        }
        
        tuple = m_tuple_list.get(cursor);
        
        // Normal form assumption: the tuple has a single attribute
        assert (tuple.getAttributeNames().size() == 1);
        assert (tuple.getAttributeNames().iterator().next().equals(Project.PROJECT_ALIAS));
        
        Value value = tuple.getAttribute(Project.PROJECT_ALIAS);
        
        binding.addValue(new BindingValue(value, true));
        
        if (reference.getOperator().getOrderVariable() != null)
        {
            binding.addValue(new BindingValue(new LongValue(m_order_counter++), false));
        }
        
        m_cursors.put(reference, ++cursor);
        return binding;
    }
    
    @Override
    public List<PhysicalPlan> getPhysicalPlansUsed()
    {
        return Collections.<PhysicalPlan> singletonList(m_assign_plan);
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitAssignImpl(this);
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        AssignMemoryToMemoryImpl copy = new AssignMemoryToMemoryImpl(getOperator());
        copy.setAssignPlan(m_assign_plan.copy(context));
        // Add the copy in context
        context.addAssignTarget(getOperator().getTarget(), copy);
        return copy;
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        super.open();
        m_cursors = new HashMap<ScanImplReferencing, Integer>();
        m_tuple_list = new LinkedList<TupleValue>();
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        m_result = qp.createLazyQueryResult(m_assign_plan, qp.getUnifiedApplicationState());
        m_result.open();
        m_order_counter = 0;
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        super.close();
        m_cursors = null;
        m_tuple_list = null;
        m_result.close();
        m_result = null;
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        Binding next_binding = m_result.next();
        if (next_binding == null) return null;
        
        OutputInfo output_info = m_assign_plan.getRootOperatorImpl().getOperator().getOutputInfo();
        TupleValue tuple = next_binding.toTupleValue(output_info);
        m_tuple_list.add(tuple);
        return next_binding;
    }
}
