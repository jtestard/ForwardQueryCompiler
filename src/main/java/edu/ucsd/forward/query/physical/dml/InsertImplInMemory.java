/**
 * 
 */
package edu.ucsd.forward.query.physical.dml;

import java.util.Iterator;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.index.IndexUtil;
import edu.ucsd.forward.data.mapping.MappingGroup;
import edu.ucsd.forward.data.mapping.MappingUtil;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.type.TupleType.AttributeEntry;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeConverter;
import edu.ucsd.forward.data.type.TypeException;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.dml.AbstractDmlOperator;
import edu.ucsd.forward.query.logical.dml.Insert;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.CopyContext;
import edu.ucsd.forward.query.physical.OperatorImpl;
import edu.ucsd.forward.query.physical.TermEvaluator;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * An implementation of the insert operator targeting an in memory data source.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class InsertImplInMemory extends AbstractInsertImpl
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(InsertImplInMemory.class);
    
    /**
     * Indicates whether the operation has been done.
     */
    private boolean             m_done;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical insert operator.
     * @param child
     *            the single child operator implementation.
     */
    public InsertImplInMemory(Insert logical, OperatorImpl child)
    {
        super(logical, child);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        super.open();
        
        m_done = false;
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        if (m_done) return null;
        
        // Get the next value from the child operator implementation
        Binding in_binding = this.getChild().next();
        
        // The mapping group
        MappingGroup group = null;
        boolean modified_context = false;
        
        int count = 0;
        
        while (in_binding != null)
        {
            // Get the input collection to insert
            CollectionValue target_collection = (CollectionValue) TermEvaluator.evaluate(getOperator().getTargetTerm(), in_binding).getValue();
            
            // Instantiate parameters
            this.instantiateBoundParameters(in_binding);
            
            // Fully execute the insert physical plan
            QueryProcessor qp = QueryProcessorFactory.getInstance();
            Value insert_value = qp.createEagerQueryResult(this.getInsertPlan(), qp.getUnifiedApplicationState()).getValue();
            // Accommodate tuple values
            if (insert_value instanceof TupleValue)
            {
                CollectionValue collection_value = new CollectionValue();
                collection_value.add(insert_value);
                insert_value = collection_value;
            }
            
            // Insert tuples into the target collection
            for (Value tup_value : ((CollectionValue) insert_value).getValues())
            {
                assert (tup_value instanceof TupleValue);
                TupleValue tuple = (TupleValue) tup_value;
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
                target_collection.add(new_tuple);
                
                // Update index
                IndexUtil.addToIndex(target_collection, new_tuple);
                
                // Update the mapped collection
                try
                {
                    modified_context = modified_context || MappingUtil.modifiesContext(target_collection);
                    MappingUtil.addTuple(target_collection, new_tuple);
                }
                catch (DataSourceException e)
                {
                    throw new QueryExecutionException(QueryExecution.DATA_SOURCE_ACCESS, e);
                }
                
                count++;
            }
            
            // Get next
            in_binding = getChild().next();
        }
        
        // Get the mapping group
        List<MappingGroup> groups = QueryProcessorFactory.getInstance().getUnifiedApplicationState().getMappingGroups();
        if (!groups.isEmpty())
        {
            group = groups.get(0);
        }
        
        // Re-evaluate the mapping input
        if (group != null && modified_context)
        {
            try
            {
                group.evaluateMappingQuery();
            }
            catch (DataSourceException e)
            {
                throw new QueryExecutionException(QueryExecution.DATA_SOURCE_ACCESS, e);
            }
        }
        
        // Create output binding
        Binding binding = new Binding();
        IntegerValue count_value = new IntegerValue(count);
        TupleValue tup_value = new TupleValue();
        tup_value.setAttribute(AbstractDmlOperator.COUNT_ATTR, count_value);
        binding.addValue(new BindingValue(tup_value, true));
        m_done = true;
        
        return binding;
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        InsertImplInMemory copy = new InsertImplInMemory(this.getOperator(), this.getChild().copy(context));
        
        copy.setInsertPlan(this.getInsertPlan().copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitInsertImpl(this);
    }
    
}
