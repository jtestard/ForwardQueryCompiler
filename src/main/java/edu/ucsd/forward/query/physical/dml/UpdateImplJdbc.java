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
import edu.ucsd.forward.data.source.jdbc.statement.UpdateStatement;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeConverter;
import edu.ucsd.forward.data.type.TypeException;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.QueryProcessor.DataSourceAccess;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.SendPlan;
import edu.ucsd.forward.query.logical.dml.AbstractDmlOperator;
import edu.ucsd.forward.query.logical.dml.Update;
import edu.ucsd.forward.query.logical.dml.Update.Assignment;
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
 * An implementation of the update logical operator targeting a JDBC data source.
 * 
 * We assume all JDBC sources have defined primary keys for their tables.
 * 
 * During initialization, the primary keys of the target Scan are computed and any missing primary keys are added to its OutputInfo.
 * Since the target Scan may be null, this operation is optional.
 * 
 * During the iteration phase, each binding in the input is used for evaluating the values for attributes in the WHERE clause of the
 * Update Statement (primary keys). The subPlan for each Assignment is executed in order to compute the new value for the columns
 * that will get updated. These values are inserted in the Update Statement and for each binding in the input, one such statement
 * mill be issued. However, the Update Statements are executed as a batch for efficiency.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * @author <Vicky Papavasileiou>
 * @author Romain Vernoux
 */
@SuppressWarnings("serial")
public class UpdateImplJdbc extends AbstractUpdateImpl
{
    @SuppressWarnings("unused")
    private static final Logger               log = Logger.getLogger(UpdateImplJdbc.class);
    
    /**
     * Indicates whether the operation has been done.
     */
    private boolean                           m_done;
    
    /**
     * The parameterized JDBC update statement to be executed repeatedly.
     */
    private UpdateStatement                   m_update_stmt;
    
    /**
     * The JDBC execution to be used.
     */
    private JdbcExecution                     m_execution;
    
    /**
     * A maps that remembers the keys to use to create the delete statements.
     */
    private Map<RelativeVariable, QueryPath>  m_keys;
    
    /**
     * A maps that remembers the name of the variable to update for each assignment.
     */
    private Map<Assignment, RelativeVariable> m_assignments_to_var;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical update operator.
     * @param child
     *            the single child operator implementation.
     */
    public UpdateImplJdbc(Update logical, OperatorImpl child)
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
        
        if (m_update_stmt == null)
        {
            m_update_stmt = new UpdateStatement((JdbcDataSource) access.getDataSource());
            m_keys = new HashMap<RelativeVariable, QueryPath>();
            m_assignments_to_var = new HashMap<Assignment, RelativeVariable>();
            
            // Set the SET clauses
            for (Assignment assignment : this.getOperator().getAssignments())
            {
                assert (assignment.getTerm() instanceof RelativeVariable);
                RelativeVariable assignment_var = (RelativeVariable) assignment.getTerm();
                
                Operator assignment_root = assignment.getLogicalPlan().getRootOperator();
                String target_scan_src = this.getOperator().getTargetScan().getExecutionDataSourceName();
                
                if (this.getOperator().getTargetScan() != null)
                {
                    Scan target_scan = this.getOperator().getTargetScan();
                    
                    // Find out the corresponding name of this variable above the target scan
                    RelativeVariable rel_var = assignment_var;
                    QueryPath qp = null;
                    Operator start_op = this.getOperator();
                    for (SendPlan send_plan : this.getOperator().getDescendantsAndSelf(SendPlan.class))
                    {
                        if (send_plan.getSendPlan().getRootOperator() == target_scan.getRoot())
                        {
                            qp = LogicalPlanUtil.getQueryPathBelowRenamings(rel_var, start_op, send_plan);
                            rel_var = null;
                            assert (qp != null);
                            start_op = send_plan.getSendPlan().getRootOperator();
                            break;
                        }
                    }
                    if (rel_var != null)
                    {
                        qp = LogicalPlanUtil.getQueryPathBelowRenamings(rel_var, start_op, target_scan);
                    }
                    else
                    {
                        qp = LogicalPlanUtil.getQueryPathBelowRenamings(qp, start_op, target_scan);
                    }
                    assert (qp != null);
                    assert (qp.getTerm().equals(target_scan.getAliasVariable()));
                    assert (qp.getPathSteps().size() == 1);
                    assignment_var = new RelativeVariable(qp.getPathSteps().get(0));
                    m_assignments_to_var.put(assignment, assignment_var);
                }
                
                // The plan of the assignment should be sent to the same database of the update
                if (assignment_root instanceof SendPlan && assignment_root.getExecutionDataSourceName().equals(target_scan_src))
                {
                    m_update_stmt.addAssignmentItemWithPlan(assignment_var, assignment.getLogicalPlan());
                }
                else
                {
                    m_update_stmt.addAssignmentItem(assignment_var);
                }
            }
            
            // Add condition for all primary keys in WHERE clause
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
                    m_update_stmt.addWhereItem(var);
                }
            }
        }
        
        m_update_stmt.setDmlStatement(this.getOperator());
        m_execution = new JdbcExecution((JdbcTransaction) access.getTransaction(), m_update_stmt);
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        if (m_done) return null;
        
        Value[] arguments = new Value[m_update_stmt.getNumberOfArguments()];
        int index = 0;
        int count = 0;
        
        // Get the next value from the child operator implementation
        Binding in_binding = this.getChild().next();
        
        // Add to the batch
        while (in_binding != null)
        {
            // Instantiate parameters
            this.instantiateBoundParameters(in_binding);
            
            QueryProcessor qp = QueryProcessorFactory.getInstance();
            
            // Assign values to SET arguments
            for (AssignmentImpl assignment_impl : this.getAssignmentImpls())
            {
                RelativeVariable assignment_var = m_assignments_to_var.get(assignment_impl.getAssignment());
                
                // Plug in the value of the evaluation of the assignment plan
                if (!m_update_stmt.hasAssignmentPlan(assignment_var))
                {
                    // Fully execute the assignment physical plan
                    Value update_value = qp.createEagerQueryResult(assignment_impl.getPhysicalPlan(),
                                                                   qp.getUnifiedApplicationState()).getValue();
                    
                    // Convert to the right type
                    try
                    {
                        Type target_type = assignment_impl.getAssignment().getTerm().inferType(getOperator().getChildren());
                        update_value = TypeConverter.getInstance().convert(update_value, target_type);
                    }
                    catch (QueryCompilationException e)
                    {
                        // FIXME Move QueryCompilationException to method Update.updateOutputInfo
                        throw new AssertionError(e);
                    }
                    catch (TypeException e)
                    {
                        // Chain the exception
                        throw new QueryExecutionException(QueryExecution.FUNCTION_EVAL_ERROR, e, this.getName());
                    }
                    m_update_stmt.setAssigmentItemValue(assignment_var, update_value.toString());
                    arguments[index++] = update_value;
                }
            }
            
            // Assign values to WHERE arguments
            for (RelativeVariable var : m_keys.keySet())
            {
                Value path_value = QueryPathEvaluator.evaluate(m_keys.get(var), in_binding).getValue();
                m_update_stmt.setWhereRightValue(var, path_value.toString());
                arguments[index++] = path_value;
            }
            
            m_execution.addBatch(arguments);
            
            if (index != 0)
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
        UpdateImplJdbc copy = new UpdateImplJdbc(this.getOperator(), this.getChild().copy(context));
        
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
