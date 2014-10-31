/**
 * 
 */
package edu.ucsd.forward.query.physical.dml;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.dml.Delete;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.CopyContext;
import edu.ucsd.forward.query.physical.OperatorImpl;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.IndexedDbSuspensionDmlRequest;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * An implementation of the delete operator targeting an IndexedDB data source.
 * 
 * @author Yupeng
 * 
 */
public class DeleteImplIdb extends AbstractDeleteImpl
{
    /**
     * Indicates whether the operation has been done.
     */
    private boolean          m_done;
    
    /**
     * Indicates whether the suspension request was sent.
     */
    private boolean          m_req_sent;
    
    private List<TupleValue> m_tuples_to_delete;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical delete operator.
     * @param child
     *            the single child operator implementation.
     */
    public DeleteImplIdb(Delete logical, OperatorImpl child)
    {
        super(logical, child);
        m_tuples_to_delete = new ArrayList<TupleValue>();
    }
    
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
        visitor.visitDeleteImpl(this);
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
        DeleteImplIdb copy = new DeleteImplIdb(this.getOperator(), this.getChild().copy(context));
        
        return copy;
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        super.open();
        m_done = false;
        m_req_sent = false;
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
        // Get the next value from the child operator implementation
        
        Binding in_binding = this.getChild().next();
        
        while (in_binding != null)
        {
            // We assume there are no empty tuples that need to be deleted
            
            //FIXME due to Scan refarctoring the methods below need to change. The implementation of this operator in general needs to change.
//            TupleValue tuple_to_delete = (TupleValue) in_binding.getValue(this.getOperator().getDeleteIndex()).getValue();
            
            // Cache the tuple
//            m_tuples_to_delete.add(tuple_to_delete);
            
            in_binding = getChild().next();
            
        }
        
        if (!m_req_sent)
        {
            IndexedDbSuspensionDmlRequest req = new IndexedDbSuspensionDmlRequest(getOperator(), m_tuples_to_delete);
            m_req_sent = true;
            throw new SuspensionException(req);
        }
        
        int count = m_tuples_to_delete.size();
        m_tuples_to_delete.clear();
        m_done = true;
        // Create output binding
        Binding binding = new Binding();
        binding.addValue(new BindingValue(new IntegerValue(count), true));
        
        return binding;
    }
}
