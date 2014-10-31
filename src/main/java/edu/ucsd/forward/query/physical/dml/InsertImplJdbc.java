/**
 * 
 */
package edu.ucsd.forward.query.physical.dml;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceMetaData.StorageSystem;
import edu.ucsd.forward.data.source.jdbc.JdbcDataSource;
import edu.ucsd.forward.data.source.jdbc.JdbcExecution;
import edu.ucsd.forward.data.source.jdbc.JdbcTransaction;
import edu.ucsd.forward.data.source.jdbc.model.ColumnHandle;
import edu.ucsd.forward.data.source.jdbc.model.Table;
import edu.ucsd.forward.data.source.jdbc.model.TableHandle;
import edu.ucsd.forward.data.source.jdbc.statement.InsertStatement;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.data.value.TupleValue.AttributeValueEntry;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.QueryProcessor.DataSourceAccess;
import edu.ucsd.forward.query.logical.dml.AbstractDmlOperator;
import edu.ucsd.forward.query.logical.dml.Insert;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.CopyContext;
import edu.ucsd.forward.query.physical.OperatorImpl;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * An implementation of the insert operator targeting a JDBC data source.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class InsertImplJdbc extends AbstractInsertImpl
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(InsertImplJdbc.class);
    
    /**
     * Indicates whether the operation has been done.
     */
    private boolean             m_done;
    
    /**
     * The parameterized JDBC insert statement to be executed repeatedly.
     */
    private InsertStatement     m_insert_stmt;
    
    /**
     * The JDBC execution to be used.
     */
    private JdbcExecution       m_execution;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical insert operator.
     * @param child
     *            the single child operator implementation.
     */
    public InsertImplJdbc(Insert logical, OperatorImpl child)
    {
        super(logical, child);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        super.open();
        
        m_done = false;
        
        // Retrieve the data source access
        DataSourceAccess access = QueryProcessorFactory.getInstance().getDataSourceAccess(
                                                                                          this.getOperator().getTargetDataSourceName());
        assert (access.getDataSource().getMetaData().getStorageSystem() == StorageSystem.JDBC);
        
        if (m_insert_stmt == null)
        {
            // Get the target table name from the target query path
            Term target_term = this.getOperator().getTargetTerm();
            String target_table_name = target_term.getDefaultProjectAlias();
            
            // Get the target table
            Table target_table = ((JdbcDataSource) access.getDataSource()).getSqlTable(target_table_name);
            
            List<ColumnHandle> column_handles = new ArrayList<ColumnHandle>();
            for (String attr_name : this.getOperator().getTargetAttributes())
            {
                column_handles.add(new ColumnHandle(target_table, attr_name));
            }
            
            m_insert_stmt = new InsertStatement(new TableHandle(target_table), column_handles);
        }
        
        m_execution = new JdbcExecution((JdbcTransaction) access.getTransaction(), m_insert_stmt);
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        if (m_done) return null;
        
        // Get the next value from the child operator implementation
        Binding in_binding = this.getChild().next();
        
        int count = 0;
        
        while (in_binding != null)
        {
            // Instantiate parameters
            this.instantiateBoundParameters(in_binding);
            
            // Fully execute the insert physical plan
            QueryProcessor qp = QueryProcessorFactory.getInstance();
            Value nested_value = qp.createEagerQueryResult(this.getInsertPlan(), qp.getUnifiedApplicationState()).getValue();
            // Accommodate tuple values
            if (nested_value instanceof TupleValue)
            {
                CollectionValue collection_value = new CollectionValue();
                collection_value.add(nested_value);
                nested_value = collection_value;
            }
            
            // Add to the batch
            for (Value value : ((CollectionValue) nested_value).getValues())
            {
                assert(value instanceof TupleValue);
                TupleValue tuple = (TupleValue) value;
                
                // Prepare the arguments using as many left most binding values as needed
                Value[] arguments = new Value[m_insert_stmt.getColumnHandles().size()];
                int i = 0;
                for (AttributeValueEntry entry : tuple)
                {
                    arguments[i++] = entry.getValue();
                }
                m_execution.addBatch(arguments);
                
                count++;
            }
            
            in_binding = getChild().next();
        }
        
        // Execute the batch
        m_execution.executeBatch();
        m_execution.end();
        
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
        InsertImplJdbc copy = new InsertImplJdbc(this.getOperator(), this.getChild().copy(context));
        
        // We need to copy the physical plan, even though it will be translated to SQL, because of different parameter
        // instantiations per plan execution.
        copy.setInsertPlan(this.getInsertPlan().copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitInsertImpl(this);
    }
    
}
