/**
 * 
 */
package edu.ucsd.forward.query.physical.dml;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.mapping.MappingGroup;
import edu.ucsd.forward.data.mapping.MappingUtil;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.dml.AbstractDmlOperator;
import edu.ucsd.forward.query.logical.dml.Delete;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.CopyContext;
import edu.ucsd.forward.query.physical.OperatorImpl;
import edu.ucsd.forward.query.physical.TermEvaluator;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * An implementation of the delete operator targeting an in memory data source.
 * 
 * We assume all in memory sources have defined primary keys for their data objects.
 * 
 * During initialization (Method open), primary keys of the CollectionValue in the input of the target Scan are computed and added
 * to the OutputInfo of the Scan, if not already there. Next, the CollectionValue from which tuples will be deleted from, is
 * retrieved using the
 * 
 * During the iteration phase, for each binding in the input the tuples that will get deleted are computed. These are the tuples in
 * the CollectionValue that match on the primary keys with the input binding.
 * 
 * All the tuples are deleted from the CollectionValue in one shot.
 * 
 * -------------> For performance issued of the developer and the code itself, for September we will go with Updating by reference
 * for in-memory data sources. This means that the bindings in the input of the Update operator shouldn't be cloned.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * @author <Vicky Papavasileiou>
 * @author Romain Vernoux
 */
@SuppressWarnings("serial")
public class DeleteImplInMemory extends AbstractDeleteImpl
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DeleteImplInMemory.class);
    
    /**
     * Indicates whether the operation has been done.
     */
    private boolean             m_done;
    
    /**
     * The CollectionValue we want to delete from. FIXME Are we sure it is always be a collection?
     */
    private CollectionValue     m_delete_from;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical delete operator.
     * @param child
     *            the single child operator implementation.
     */
    public DeleteImplInMemory(Delete logical, OperatorImpl child)
    {
        super(logical, child);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        super.open();
        
        // Find the primary keys for the scan over the table we want to delete from
        // If a key is not in the outputinfo of the scan, then add it
        
        // FIXME uncomment when primary keys are used. Actually, only for Delete I need primary keys in nested collections
        // PhysicalPlanUtil.inferAndAddKeysToScan(this.getOperator(), this.getOperator().getTargetScan());
        //
        // // Get CollectionValue of data object we want to delete from
        // Term term = this.getOperator().getTargetScan().getOutputInfo().getProvenanceTerm(
        // this.getOperator().getTargetScan().getAliasVariable());
        // Value scan_value = TermEvaluator.evaluate(term, new Binding()).getValue();
        // if (scan_value instanceof ScalarValue || scan_value instanceof TupleValue)
        // {
        // m_delete_from = (CollectionValue) scan_value.getParent();
        // }
        // else
        // {
        // assert (scan_value instanceof CollectionValue); // FIXME: How do you delete from Json and Java Types?
        // m_delete_from = (CollectionValue) scan_value;
        // }
        
        m_done = false;
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        if (m_done) return null;
        
        int count = 0;
        
        // Get the next value from the child operator implementation
        Binding in_binding = this.getChild().next();
        
        // The mapping group
        MappingGroup group = null;
        boolean modified_context = false;
        
        Set<Value> tuples_to_delete = new HashSet<Value>();
        
        while (in_binding != null)
        {            
            Variable var = this.getOperator().getTargetScan().getAliasVariable();
            
            Value tuple = in_binding.getValue(var.getBindingIndex()).getValue();
            
            assert (tuple instanceof TupleValue);
            
            // Cache the tuple
            tuples_to_delete.add((TupleValue) tuple);
            
            count++;
            
            in_binding = getChild().next();
        }
        
        for (Value value : tuples_to_delete)
        {
            Value parent = value.getParent();
            
            if (parent instanceof CollectionValue)
            {
                ((CollectionValue) parent).remove(value);
                
                // Update the mapped collection
                try
                {
                    modified_context = modified_context || MappingUtil.modifiesContext(parent);
                    MappingUtil.removeTuple((CollectionValue) parent, (TupleValue) value);
                }
                catch (DataSourceException e)
                {
                    throw new QueryExecutionException(QueryExecution.DATA_SOURCE_ACCESS, e);
                }
            }
            else if (parent instanceof TupleValue) // nested case
            {
                CollectionValue new_value = new CollectionValue();
                
                // Update the mapped tuple
                try
                {
                    modified_context = modified_context || MappingUtil.modifiesContext(value);
                    MappingUtil.updateValue(value, new_value);
                }
                catch (DataSourceException e)
                {
                    throw new QueryExecutionException(QueryExecution.DATA_SOURCE_ACCESS, e);
                }
                
                ((TupleValue) parent).setAttribute(this.getOperator().getTargetScan().getTerm().getDefaultProjectAlias(),
                                                   new_value);
            }
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
        DeleteImplInMemory copy = new DeleteImplInMemory(this.getOperator(), this.getChild().copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitDeleteImpl(this);
    }
    
}
