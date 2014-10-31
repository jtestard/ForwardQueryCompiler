/**
 * 
 */
package edu.ucsd.forward.query;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.UncheckedException;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.SendPlanImpl;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.suspension.SuspensionException;
import edu.ucsd.forward.query.suspension.SuspensionHandler;
import edu.ucsd.forward.query.suspension.SuspensionRequest;
import edu.ucsd.forward.query.suspension.SuspensionRequestDispatcher;

/**
 * A eagerly executed query result, which caches the result.
 * 
 * @author Yupeng
 * 
 */
public class EagerQueryResult implements QueryResult
{
    @SuppressWarnings("unused")
    private static final Logger log          = Logger.getLogger(EagerQueryResult.class);
    
    private LazyQueryResult     m_lazy_result;
    
    private CollectionValue     m_collection = null;
    
    /**
     * Constructs the eager query result using the lazy query result.
     * 
     * @param lazy_result
     *            the lazy query result.
     */
    public EagerQueryResult(LazyQueryResult lazy_result)
    {
        assert (lazy_result != null);
        m_lazy_result = lazy_result;
    }
    
    /**
     * Executes a physical query plan eagerly and returns the resulting value.
     * 
     * Note: The method assumes that the unified application state has been already set.
     * 
     * @return the eagerly executed value.
     * @throws QueryExecutionException
     *             if the execution of the input physical query plan raises an exception.
     */
    public Value getValue() throws QueryExecutionException
    {
        try
        {
            return getValueWithSuspension();
        }
        catch (SuspensionException e)
        {
            SuspensionRequest request = e.getRequest();
            SuspensionHandler handler = SuspensionRequestDispatcher.getInstance().dispatch(request);
            handler.handle();
            
            return getValue();
        }
    }
    
    /**
     * Executes a physical query plan eagerly and returns the resulting value. The execution is allowed to be suspended.
     * 
     * Note: The method assumes that the unified application state has been already set.
     * 
     * @return the eagerly executed value.
     * @throws QueryExecutionException
     *             if the execution of the input physical query plan raises an exception.
     * @throws SuspensionException
     *             if the execution is suspended.
     */
    public Value getValueWithSuspension() throws QueryExecutionException, SuspensionException
    {
        try
        {
            PhysicalPlan physical_plan = m_lazy_result.getPhysicalPlan();
            
            Value eager_result = null;
            
            // Build the buffer
            if (m_collection == null)
            {
                m_collection = new CollectionValue();
                m_lazy_result.open();
            }
            
            Binding next_binding = m_lazy_result.next();
            
            assert (physical_plan.getRootOperatorImpl() instanceof SendPlanImpl);
            SendPlanImpl send_plan = (SendPlanImpl) physical_plan.getRootOperatorImpl();
            boolean output_ordered = send_plan.getSendPlan().getRootOperatorImpl().getOperator().getOutputInfo().isOutputOrdered();
            m_collection.setOrdered(output_ordered);
            while (next_binding != null)
            {
                m_collection.add(next_binding.toValue());
                
                next_binding = m_lazy_result.next();
            }
            
            m_lazy_result.close();
            
            // Unwrap the value in the only tuple if necessary
            if (physical_plan.getLogicalPlan().isWrapping())
            {
                if (m_collection.size() == 0)
                {
                    CollectionType output_type = physical_plan.getLogicalPlan().getOutputType();
                    Type type_class = output_type.getChildrenType();
                    assert(type_class instanceof TupleType);
                    
                    eager_result = new NullValue(((TupleType) type_class).iterator().next().getType().getClass());
                }
                else
                {
                    assert (m_collection.size() == 1);
                    eager_result = m_collection.getValues().get(0);
                    assert (eager_result instanceof TupleValue);
                    TupleValue tuple = (TupleValue) eager_result;
                    assert (tuple.getSize() == 1);
                    eager_result = tuple.removeAttribute(tuple.getAttributeNames().iterator().next());
                }
            }
            else
            {
                eager_result = m_collection;
            }
            
            return eager_result;
        }
        catch (Throwable t)
        {
            if (t instanceof SuspensionException) throw (SuspensionException) t;
            
            // Propagate abnormal conditions immediately
            if (t instanceof java.lang.Error && !(t instanceof AssertionError)) throw (java.lang.Error) t;
            
            // Cleanup
            QueryProcessorFactory.getInstance().cleanup(true);
            
            // Throw the expected exception
            if (t instanceof QueryExecutionException) throw (QueryExecutionException) t;
            
            // Propagate the unexpected exception
            throw (t instanceof UncheckedException) ? (UncheckedException) t : new UncheckedException(t);
        }
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        // DO NOTHING
    }
    
    @Override
    public Binding next() throws QueryExecutionException
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        // DO NOTHING
    }
}
