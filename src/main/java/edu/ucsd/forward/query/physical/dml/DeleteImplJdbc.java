/**
 * 
 */
package edu.ucsd.forward.query.physical.dml;

import java.util.HashMap;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.constraint.LocalPrimaryKeyConstraint;
import edu.ucsd.forward.data.source.DataSourceMetaData.StorageSystem;
import edu.ucsd.forward.data.source.jdbc.JdbcDataSource;
import edu.ucsd.forward.data.source.jdbc.JdbcExecution;
import edu.ucsd.forward.data.source.jdbc.JdbcTransaction;
import edu.ucsd.forward.data.source.jdbc.statement.DeleteStatement;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.QueryProcessor.DataSourceAccess;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.SendPlan;
import edu.ucsd.forward.query.logical.dml.AbstractDmlOperator;
import edu.ucsd.forward.query.logical.dml.Delete;
import edu.ucsd.forward.query.logical.plan.LogicalPlanUtil;
import edu.ucsd.forward.query.logical.term.ElementVariable;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.CopyContext;
import edu.ucsd.forward.query.physical.OperatorImpl;
import edu.ucsd.forward.query.physical.QueryPathEvaluator;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * An implementation of the delete operator targeting a JDBC data source.
 * 
 * We assume all JDBC sources have defined primary keys for their tables.
 * 
 * During initialization, the primary keys of the target Scan are computed and any missing primary keys are added to its OutputInfo.
 * This is needed only when the Delete Statement is populated in memory. If the entire Delete plan is executed by a database, the
 * inference of primary keys in not necessary. If this is the case, the PlanToAstTranslator is used to translate the plan to a
 * Select query and next the Select is replaced by a Delete.
 * 
 * During the iteration phase, each binding in the input is used for evaluating the values for the primary keys. These values are
 * inserted in the Delete Statement and for each binding in the input, one such statement mill be issued in a batch mode. If the
 * entire plan is executed by a database, the input binding is not used.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * @author <Vicky Papavasileiou>
 * @author Romain Vernoux
 */
@SuppressWarnings("serial")
public class DeleteImplJdbc extends AbstractDeleteImpl
{
    @SuppressWarnings("unused")
    private static final Logger              log = Logger.getLogger(DeleteImplJdbc.class);
    
    /**
     * Indicates whether the operation has been done.
     */
    private boolean                          m_done;
    
    /**
     * The parameterized JDBC delete statement to be executed repeatedly.
     */
    private DeleteStatement                  m_delete_stmt;
    
    /**
     * The JDBC execution to be used.
     */
    private JdbcExecution                    m_execution;
    
    /**
     * A maps that remembers the keys to use to create the delete statements.
     */
    private Map<RelativeVariable, QueryPath> m_keys;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical delete operator.
     * @param child
     *            the single child operator implementation.
     */
    public DeleteImplJdbc(Delete logical, OperatorImpl child)
    {
        super(logical, child);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        super.open();
        
        m_done = false;
        
        // Retrieve the data source access
        String target_src_name = this.getOperator().getTargetDataSourceName();
        DataSourceAccess access = QueryProcessorFactory.getInstance().getDataSourceAccess(target_src_name);
        assert (access.getDataSource().getMetaData().getStorageSystem() == StorageSystem.JDBC);
        
        if (m_delete_stmt == null)
        {
            m_delete_stmt = new DeleteStatement((JdbcDataSource) access.getDataSource());
            m_keys = new HashMap<RelativeVariable, QueryPath>();
            
            if (this.getOperator().getTargetScan() != null)
            {
                Term scan_term = this.getOperator().getTargetScan().getTerm();
                Type scan_type = scan_term.getType();
                ElementVariable scan_var = this.getOperator().getTargetScan().getAliasVariable();
                
                assert (scan_type instanceof CollectionType);
                CollectionType col_type = (CollectionType) scan_type;
                
                // Get the primary keys
                LocalPrimaryKeyConstraint constraint = col_type.getLocalPrimaryKeyConstraint();
                
                if (constraint == null)
                {
                    // FIXME Handle deletion when the underlying table has no primary key
                    throw new UnsupportedOperationException();
                }
                
                // For each key, find out the corresponding path for this operator, and add the query path to the map of keys
                for (SchemaPath sp : constraint.getAttributesSchemaPaths())
                {
                    RelativeVariable rel_var = new RelativeVariable(sp.relative(1).getPathSteps().get(0));
                    QueryPath old_qp = new QueryPath(scan_var, sp.relative(1).getPathSteps());
                    QueryPath qp = old_qp;
                    Operator start_op = this.getOperator().getTargetScan();
                    for (SendPlan send_plan : this.getOperator().getDescendantsAndSelf(SendPlan.class))
                    {
                        if (send_plan.getSendPlan().getRootOperator() == this.getOperator().getTargetScan().getRoot())
                        {
                            qp = LogicalPlanUtil.getQueryPathAboveRenamings(qp, start_op,
                                                                            send_plan.getSendPlan().getRootOperator(), false);
                            assert (qp != null);
                            start_op = send_plan;
                            break;
                        }
                    }
                    qp = LogicalPlanUtil.getQueryPathAboveRenamings(qp, start_op, this.getOperator(), true);
                    assert (qp != null);
                    m_keys.put(rel_var, qp);
                }
                
                // Add condition for all primary keys in WHERE clause
                for (RelativeVariable var : m_keys.keySet())
                {
                    m_delete_stmt.addWhereItem(var);
                }
                
            }
        }
        
        m_delete_stmt.setDmlStatement(this.getOperator());
        m_execution = new JdbcExecution((JdbcTransaction) access.getTransaction(), m_delete_stmt);
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        if (m_done) return null;
        
        Value[] arguments = new Value[m_delete_stmt.getNumberOfArguments()];
        int index = 0;
        int count = 0;
        
        // Get the next value from the child operator implementation
        Binding in_binding = this.getChild().next();
        
        // Add to the batch
        while (in_binding != null)
        {
            // Assign values to WHERE arguments
            for (RelativeVariable var : m_keys.keySet())
            {
                Value path_value = QueryPathEvaluator.evaluate(m_keys.get(var), in_binding).getValue();
                m_delete_stmt.setWhereRightValue(var, path_value.toString());
                arguments[index++] = path_value;
            }
            
            m_execution.addBatch(arguments);
            
            // Since the input binding contains the primary key of the tuple we want to delete, when do we need to do getNext?
            if (m_delete_stmt.getNumberOfArguments() != 0)
            {
                // Get next
                in_binding = getChild().next();
                index = 0;
            }
            else
            {
                in_binding = null;
            }
            count++;
        }
        
        // Execute the batch
        m_execution.executeBatch();
        m_execution.end();
        
        // Create a binding
        Binding binding = new Binding();
        TupleValue tup_value = new TupleValue();
        tup_value.setAttribute(AbstractDmlOperator.COUNT_ATTR, new IntegerValue(count));
        binding.addValue(new BindingValue(tup_value, true));
        m_done = true;
        
        return binding;
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        DeleteImplJdbc copy = new DeleteImplJdbc(this.getOperator(), this.getChild().copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitDeleteImpl(this);
    }
    
}
