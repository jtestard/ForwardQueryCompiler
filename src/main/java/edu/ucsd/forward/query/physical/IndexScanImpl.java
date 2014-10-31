/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.index.KeyRange;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessor.DataSourceAccess;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.IndexScan;
import edu.ucsd.forward.query.logical.term.AbsoluteVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Implementation of the index scan operator.
 * 
 * @author Yupeng
 * 
 */
public class IndexScanImpl extends AbstractUnaryOperatorImpl<IndexScan>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(IndexScanImpl.class);
    
    /**
     * The tuple value iterator.
     */
    private LinkedList<Value>   m_tuple_list;
    
    /**
     * The current input binding.
     */
    private Binding             m_in_binding;
    
    /**
     * The list of variables to retrieve from the input binding.
     */
    private List<Variable>      m_variables;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical index scan operator.
     * @param child
     *            the single child operator implementation.
     */
    public IndexScanImpl(IndexScan logical, OperatorImpl child)
    {
        super(logical, child);
        
        m_variables = new ArrayList<Variable>(logical.getOutputInfo().getVariables());
        
        // Skip the variables of the incoming binding
        m_variables.removeAll(child.getOperator().getOutputInfo().getVariables());
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitIndexScanImpl(this);
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        IndexScanImpl copy = new IndexScanImpl(getOperator(), getChild().copy(context));
        
        copy.m_variables = m_variables;
        return copy;
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        super.open();
        
        m_in_binding = null;
        m_tuple_list = new LinkedList<Value>();
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        // Get the next tuple value
        Value tuple_value = m_tuple_list.poll();
        
        if (tuple_value == null)
        {
            while (true)
            {
                m_in_binding = this.getChild().next();
                
                // Fail fast if there is no input binding
                if (m_in_binding == null)
                {
                    return null;
                }
                
                // Reset the cloned flags since binding values are used in multiple bindings
                m_in_binding.resetCloned();
                
                // Read from the index
                Term term = this.getOperator().getTerm();
                // FIXME currently the index is only on the root collection.
                assert term instanceof AbsoluteVariable;
                AbsoluteVariable absolute_var = (AbsoluteVariable) term;
                String source_name = absolute_var.getDataSourceName();
                String data_obj_name = absolute_var.getSchemaObjectName();
                
                // Retrieve the data source access
                DataSourceAccess access = QueryProcessorFactory.getInstance().getDataSourceAccess(source_name);
                SchemaPath collection_path = SchemaPath.root();
                
                List<KeyRange> ranges = KeyRangeEvaluator.evaluate(m_in_binding, getOperator().getKeyRangeSpecs());
                List<TupleValue> value = access.getDataSource().getFromIndex(data_obj_name, collection_path,
                                                                             getOperator().getIndexName(), ranges);
                
                m_tuple_list.addAll(value);
                
                // Get the next tuple value
                tuple_value = m_tuple_list.poll();
                
                if (tuple_value == null)
                {
                    // Keep fetching input bindings until the query path evaluation is not empty (inner join semantics)
                    continue;
                }
                
                break;
            }
        }
        
        // Create a new binding
        Binding binding = new Binding();
        
        // Add the tuple value
        binding.addValue(new BindingValue(tuple_value, false));
        
        return binding;
        
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        if (this.getState() == State.OPEN)
        {
            m_tuple_list = null;
            m_in_binding = null;
        }
        
        super.close();
    }
}
