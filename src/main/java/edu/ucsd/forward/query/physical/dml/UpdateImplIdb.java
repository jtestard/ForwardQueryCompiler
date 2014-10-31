/**
 * 
 */
package edu.ucsd.forward.query.physical.dml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeConverter;
import edu.ucsd.forward.data.type.TypeException;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.EagerQueryResult;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.dml.Update;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.CopyContext;
import edu.ucsd.forward.query.physical.OperatorImpl;
import edu.ucsd.forward.query.physical.TermEvaluator;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.IndexedDbSuspensionDmlRequest;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * An implementation of the update logical operator targeting an IndexedDB data source.
 * 
 * @author Yupeng
 * 
 */
public class UpdateImplIdb extends AbstractUpdateImpl
{
    
    /**
     * Indicates whether the operation has been done.
     */
    private boolean           m_done;
    
    /**
     * Indicates whether the update has been done for the current binding.
     */
    private boolean           m_binding_done;
    
    /**
     * The current binding.
     */
    private Binding           m_in_binding;
    
    /**
     * Caches the pairs of old value and new value for update per binding.
     */
    private Map<Value, Value> m_values_to_update;
    
    /**
     * Indicates the index of assignment implementation under processing.
     */
    private int               m_idx;
    
    /**
     * Indicates whether the suspension request was sent.
     */
    private boolean           m_req_sent;
    
    /**
     * Indicates the tuples to update.
     */
    private List<TupleValue>  m_tuples_to_update;
    
    private EagerQueryResult  m_query_result;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical update operator.
     * @param child
     *            the single child operator implementation.
     */
    public UpdateImplIdb(Update logical, OperatorImpl child)
    {
        super(logical, child);
        m_values_to_update = new HashMap<Value, Value>();
        m_tuples_to_update = new ArrayList<TupleValue>();
    }
    
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(UpdateImplIdb.class);
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.query.physical.OperatorImpl#accept(edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor)
     */
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitUpdateImpl(this);
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.query.physical.OperatorImpl#copy()
     */
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        UpdateImplIdb copy = new UpdateImplIdb(this.getOperator(), this.getChild().copy(context));
        
        for (AssignmentImpl assignment_impl : this.getAssignmentImpls())
        {
            copy.addAssignmentImpl(assignment_impl.copy(context));
        }
        
        return copy;
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        super.open();
        m_done = false;
        m_req_sent = false;
        m_binding_done = true;
        m_in_binding = null;
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.query.physical.Pipeline#next()
     */
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        if (m_done) return null;
        
        if (m_req_sent)
        {
            // Create output binding
            int count = m_tuples_to_update.size();
            Binding binding = new Binding();
            binding.addValue(new BindingValue(new IntegerValue(count), true));
            
            // Clear the state for this update
            m_tuples_to_update.clear();
            m_done = true;
            
            return binding;
        }
        
        if (m_binding_done)
        {
            // Get the next value from the child operator implementation
            m_in_binding = this.getChild().next();
        }
        
        while (!m_binding_done || m_in_binding != null)
        {
            if (m_binding_done && m_in_binding != null)
            {
                m_binding_done = false;
                // Instantiate parameters
                this.instantiateBoundParameters(m_in_binding);
                m_idx = 0;
            }
            
            // Resume the execution
            QueryProcessor qp = QueryProcessorFactory.getInstance();
            TupleValue tuple_to_update = null;
            for (int i = m_idx; i < getAssignmentImpls().size(); i++)
            {
                AssignmentImpl assignment_impl = getAssignmentImpls().get(i);
                // Save the current index
                m_idx = i;
                if (m_query_result == null) m_query_result = qp.createEagerQueryResult(assignment_impl.getPhysicalPlan(),
                                                                                       qp.getUnifiedApplicationState());
                
                Value new_value = m_query_result.getValue();
                // Get the target value
                Value old_value = TermEvaluator.evaluate(assignment_impl.getAssignment().getTerm(), m_in_binding).getValue();
                // The modified data is from relational collection.
                assert old_value.getParent() instanceof TupleValue;
                if (tuple_to_update == null) tuple_to_update = (TupleValue) old_value.getParent();
                
                // Convert the provided value to the target type
                try
                {
                    Type target_type = assignment_impl.getAssignment().getTerm().inferType(getOperator().getChildren());
                    new_value = TypeConverter.getInstance().convert(new_value, target_type);
                    // Detach the new value in case its parent is not null after type conversion (for example, when converting
                    // collection to scalar)
                    if (new_value.getParent() != null)
                    {
                        ValueUtil.detach(new_value);
                    }
                }
                catch (QueryCompilationException e)
                {
                    // FIXME Move QueryCompilationException to method Update.updateOutputInfo
                    assert false;
                }
                catch (TypeException e)
                {
                    // Chain the exception
                    throw new QueryExecutionException(QueryExecution.FUNCTION_EVAL_ERROR, e, this.getName());
                }
                
                m_values_to_update.put(old_value, new_value);
                m_query_result = null;
            }
            
            // Update all values in one shot
            for (Map.Entry<Value, Value> entry : m_values_to_update.entrySet())
            {
                ValueUtil.replace(entry.getKey(), entry.getValue());
            }
            
            // Store the tuple to update in buffer
            m_tuples_to_update.add(tuple_to_update);
            // Clear the state for the current binding.
            m_values_to_update.clear();
            m_binding_done = true;
            
            // Get the next binding
            m_in_binding = this.getChild().next();
        }
        
        IndexedDbSuspensionDmlRequest req = new IndexedDbSuspensionDmlRequest(getOperator(), m_tuples_to_update);
        m_req_sent = true;
        throw new SuspensionException(req);
    }
}
