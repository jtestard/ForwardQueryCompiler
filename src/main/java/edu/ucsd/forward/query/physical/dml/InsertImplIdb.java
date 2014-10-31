/**
 * 
 */
package edu.ucsd.forward.query.physical.dml;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeConverter;
import edu.ucsd.forward.data.type.TypeException;
import edu.ucsd.forward.data.type.TupleType.AttributeEntry;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.EagerQueryResult;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.dml.Insert;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.CopyContext;
import edu.ucsd.forward.query.physical.OperatorImpl;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.IndexedDbSuspensionDmlRequest;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * An implementation of the insert operator targeting an IndexedDB data source.
 * 
 * @author Yupeng
 * 
 */
public class InsertImplIdb extends AbstractInsertImpl
{
    
    /**
     * Indicates whether the operation has been done.
     */
    private boolean          m_done;
    
    /**
     * The result holds the nested plan result.
     */
    private EagerQueryResult m_query_result;
    
    /**
     * The number of inserted tuples .
     */
    private int              m_count;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical insert operator.
     * @param child
     *            the single child operator implementation.
     */
    public InsertImplIdb(Insert logical, OperatorImpl child)
    {
        super(logical, child);
    }
    
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(InsertImplIdb.class);
    
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
        visitor.visitInsertImpl(this);
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
        InsertImplIdb copy = new InsertImplIdb(this.getOperator(), this.getChild().copy(context));
        copy.setInsertPlan(this.getInsertPlan().copy(context));
        
        return copy;
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        super.open();
        m_count = 0;
        m_done = false;
        m_query_result = null;
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
        
        Binding in_binding = null;
        if (m_query_result == null)
        {
            in_binding = this.getChild().next();
            
            if (in_binding != null)
            {
                // Instantiate parameters
                this.instantiateBoundParameters(in_binding);
                // Fully execute the insert physical plan
                QueryProcessor qp = QueryProcessorFactory.getInstance();
                m_query_result = qp.createEagerQueryResult(this.getInsertPlan(), qp.getUnifiedApplicationState());
            }
        }
        
        if (m_query_result != null)
        {
            Value nested_value = m_query_result.getValue();
            
            // Accommodate tuple values
            if (nested_value instanceof TupleValue)
            {
                CollectionValue collection_value = new CollectionValue();
                collection_value.add((TupleValue) nested_value);
                nested_value = collection_value;
            }
            
            List<TupleValue> payload = new ArrayList<TupleValue>();
            
            // Insert tuples into the target collection
            for (TupleValue tuple : new ArrayList<TupleValue>(((CollectionValue) nested_value).getTuples()))
            {
                TupleValue new_tuple = new TupleValue();
                
                // Set the values of target attributes while keeping in mind that provided attributes might be named differently
                // than the target attributes.
                Iterator<String> target_iter = this.getOperator().getTargetAttributes().iterator();
                Iterator<String> provided_iter = tuple.getAttributeNames().iterator();
                while (target_iter.hasNext())
                {
                    Value value = tuple.getAttribute(provided_iter.next());
                    ValueUtil.detach(value);
                    
                    String target_attr_name = target_iter.next();
                    
                    // Convert the provided value to the target type
                    try
                    {
                        Type target_type = this.getOperator().getTargetTupleType().getAttribute(target_attr_name);
                        value = TypeConverter.getInstance().convert(value, target_type);
                    }
                    catch (TypeException e)
                    {
                        // Chain the exception
                        throw new QueryExecutionException(QueryExecution.FUNCTION_EVAL_ERROR, e, this.getName());
                    }
                    
                    new_tuple.setAttribute(target_attr_name, value);
                }
                
                // Set the values of the rest of the attributes of the target tuple type to null or default
                for (AttributeEntry entry : this.getOperator().getTargetTupleType())
                {
                    String attribute_name = entry.getName();
                    if (this.getOperator().getTargetAttributes().contains(attribute_name)) continue;
                    
                    Type value_type = entry.getType();
                    
                    // An attribute name that occurs in the tuple type but is missing in the tuple is assumed to get its default
                    // value.
                    Value value = ValueUtil.cloneNoParentNoType(value_type.getDefaultValue());
                    new_tuple.setAttribute(attribute_name, value);
                }
                payload.add(new_tuple);
                
                m_count++;
            }
            
            m_query_result = null;
            IndexedDbSuspensionDmlRequest req = new IndexedDbSuspensionDmlRequest(getOperator(), payload);
            throw new SuspensionException(req);
            
        }
        
        // Create output binding
        Binding binding = new Binding();
        binding.addValue(new BindingValue(new IntegerValue(m_count), true));
        m_done = true;
        
        return binding;
    }
}
