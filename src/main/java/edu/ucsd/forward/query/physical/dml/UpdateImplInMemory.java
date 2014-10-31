/**
 * 
 */
package edu.ucsd.forward.query.physical.dml;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.index.IndexUtil;
import edu.ucsd.forward.data.mapping.MappingGroup;
import edu.ucsd.forward.data.mapping.MappingUtil;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeConverter;
import edu.ucsd.forward.data.type.TypeException;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.dml.AbstractDmlOperator;
import edu.ucsd.forward.query.logical.dml.Update;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.CopyContext;
import edu.ucsd.forward.query.physical.OperatorImpl;
import edu.ucsd.forward.query.physical.TermEvaluator;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * An implementation of the update operator targeting an in memory data source.
 * 
 * We assume all in memory sources have defined primary keys for their data objects.
 * 
 * During initialization (Method open), primary keys of the CollectionValue in the input of the target Scan are computed and added
 * to the OutputInfo of the Scan, if not already there. Next, the CollectionValue that will get updated is retrieved using the
 * target Scan operator. This collection value is computed by evaluating the ProvenanceTerm of the Scan operator. Since the target
 * Scan may be null, the CollectionValue may be null as well and hence not used for updating.
 * 
 * During the iteration phase, for each binding in the input, the tuples that will get updated are computed. If the CollectionValue
 * is not null the tuples that will be updated are found using equality checking on the primary keys. On the other hand, if the
 * CollectionValue is null, the incoming binding is the tuple that will get updated. The new value is computed by evaluating the
 * subPlan of each assignment whereas the old value is the attribute that is the target of the assignment.
 * 
 * The old and new values pairs of all tuples are computed and stored. Next, in one shot the replacements happen using
 * ValueUtils.replace.
 * 
 * -------------> For performance issued of the developer and the code itself, for September we will go with Updating by reference
 * for in-memory data sources. This means that the bindings in the input of the Update operator shouldn't be cloned.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * @author <Vicky Papavasileiou>
 */
@SuppressWarnings("serial")
public class UpdateImplInMemory extends AbstractUpdateImpl
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(UpdateImplInMemory.class);
    
    /**
     * Indicates whether the operation has been done.
     */
    private boolean             m_done;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical update operator.
     * @param child
     *            the single child operator implementation.
     */
    public UpdateImplInMemory(Update logical, OperatorImpl child)
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
        
        Map<Value, Value> values_to_update = new HashMap<Value, Value>();
        Set<TupleValue> tuples_to_update_index = new HashSet<TupleValue>();
        
        // Get the next value from the child operator implementation
        Binding in_binding = this.getChild().next();
        
        int count = 0;
        
        while (in_binding != null)
        {
            // Instantiate parameters
            this.instantiateBoundParameters(in_binding);
            
            QueryProcessor qp = QueryProcessorFactory.getInstance();
            UnifiedApplicationState uas = qp.getUnifiedApplicationState();
            
            for (AssignmentImpl assignment_impl : this.getAssignmentImpls())
            {
                // Fully execute the assignment physical plan
                Value new_value = qp.createEagerQueryResult(assignment_impl.getPhysicalPlan(), uas).getValue();
                
                Value old_value = TermEvaluator.evaluate(assignment_impl.getAssignment().getTerm(), in_binding).getValue();
                
                // Check if need update the index
                Type value_type = assignment_impl.getAssignment().getTerm().getType();
                if (IndexUtil.getIndexDeclaration(value_type) != null)
                {
                    // FIXME we only support index built on root relation.
                    assert old_value.getParent() instanceof TupleValue;
                    tuples_to_update_index.add((TupleValue) old_value.getParent());
                }
                
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
                    throw new AssertionError(e);
                }
                catch (TypeException e)
                {
                    // Chain the exception
                    throw new QueryExecutionException(QueryExecution.FUNCTION_EVAL_ERROR, e, this.getName());
                }
                
                values_to_update.put(old_value, new_value);
            }
            
            // Get next
            in_binding = getChild().next();
            
            count++;
        }
        
        // Update the index, remove the old tuples
        for (TupleValue tuple : tuples_to_update_index)
        {
            CollectionValue collection = (CollectionValue) tuple.getParent();
            IndexUtil.removeFromIndex(collection, tuple);
        }
        
        boolean modified_context = false;
        
        // Update all values in one shot
        for (Map.Entry<Value, Value> entry : values_to_update.entrySet())
        {
            try
            {
                modified_context = modified_context || MappingUtil.modifiesContext(entry.getKey());
                
                // Update all mapped values
                MappingUtil.updateValue(entry.getKey(), entry.getValue());
            }
            catch (DataSourceException e)
            {
                throw new QueryExecutionException(QueryExecution.DATA_SOURCE_ACCESS, e);
            }
            // Check if the value was replaced, if not replace it here
            if (entry.getKey().getParent().getChildren().contains(entry.getKey()))
            {
                ValueUtil.replace(entry.getKey(), entry.getValue());
            }
        }
        
        // Update the index, add the old tuples
        for (TupleValue tuple : tuples_to_update_index)
        {
            CollectionValue collection = (CollectionValue) tuple.getParent();
            IndexUtil.addToIndex(collection, tuple);
        }
        
        // Get the mapping group
        MappingGroup group = null;
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
        
        // FIXME Validate target collection value
        
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
        UpdateImplInMemory copy = new UpdateImplInMemory(this.getOperator(), this.getChild().copy(context));
        
        for (AssignmentImpl assignment_impl : this.getAssignmentImpls())
        {
            copy.addAssignmentImpl(assignment_impl.copy(context));
        }
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitUpdateImpl(this);
    }
    
}
