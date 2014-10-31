/**
 * 
 */
package edu.ucsd.forward.query.logical.visitors;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.query.logical.AntiSemiJoin;
import edu.ucsd.forward.query.logical.ApplyPlan;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.logical.Copy;
import edu.ucsd.forward.query.logical.EliminateDuplicates;
import edu.ucsd.forward.query.logical.Exists;
import edu.ucsd.forward.query.logical.Ground;
import edu.ucsd.forward.query.logical.GroupBy;
import edu.ucsd.forward.query.logical.IndexScan;
import edu.ucsd.forward.query.logical.InnerJoin;
import edu.ucsd.forward.query.logical.Navigate;
import edu.ucsd.forward.query.logical.OffsetFetch;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.OuterJoin;
import edu.ucsd.forward.query.logical.PartitionBy;
import edu.ucsd.forward.query.logical.Product;
import edu.ucsd.forward.query.logical.Project;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.Select;
import edu.ucsd.forward.query.logical.SemiJoin;
import edu.ucsd.forward.query.logical.SendPlan;
import edu.ucsd.forward.query.logical.SetOperator;
import edu.ucsd.forward.query.logical.Sort;
import edu.ucsd.forward.query.logical.Subquery;
import edu.ucsd.forward.query.logical.ddl.CreateDataObject;
import edu.ucsd.forward.query.logical.ddl.DropDataObject;
import edu.ucsd.forward.query.logical.dml.Delete;
import edu.ucsd.forward.query.logical.dml.Insert;
import edu.ucsd.forward.query.logical.dml.Update;
import edu.ucsd.forward.query.logical.dml.Update.Assignment;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.plan.LogicalPlanUtil;
import edu.ucsd.forward.query.logical.term.AbsoluteVariable;
import edu.ucsd.forward.query.physical.AbstractAssignImpl;
import edu.ucsd.forward.query.physical.AbstractCopyImpl;
import edu.ucsd.forward.query.physical.AbstractSendPlanImpl;
import edu.ucsd.forward.query.physical.AntiSemiJoinImplNestedLoop;
import edu.ucsd.forward.query.physical.ApplyPlanImpl;
import edu.ucsd.forward.query.physical.AssignImplJDBC;
import edu.ucsd.forward.query.physical.AssignMemoryToMemoryImpl;
import edu.ucsd.forward.query.physical.CopyImplInMemory;
import edu.ucsd.forward.query.physical.EliminateDuplicatesImpl;
import edu.ucsd.forward.query.physical.ExceptImpl;
import edu.ucsd.forward.query.physical.ExistsImpl;
import edu.ucsd.forward.query.physical.GroundImpl;
import edu.ucsd.forward.query.physical.GroupByImpl;
import edu.ucsd.forward.query.physical.IndexScanImpl;
import edu.ucsd.forward.query.physical.InnerJoinImplNestedLoop;
import edu.ucsd.forward.query.physical.IntersectImpl;
import edu.ucsd.forward.query.physical.NavigateImpl;
import edu.ucsd.forward.query.physical.OffsetFetchImpl;
import edu.ucsd.forward.query.physical.OperatorImpl;
import edu.ucsd.forward.query.physical.OuterJoinImplNestedLoop;
import edu.ucsd.forward.query.physical.OuterUnionImpl;
import edu.ucsd.forward.query.physical.PartitionByImpl;
import edu.ucsd.forward.query.physical.ProductImplNestedLoop;
import edu.ucsd.forward.query.physical.ProjectImpl;
import edu.ucsd.forward.query.physical.ScanImpl;
import edu.ucsd.forward.query.physical.ScanImplReferencing;
import edu.ucsd.forward.query.physical.SelectImplSeq;
import edu.ucsd.forward.query.physical.SemiJoinImplNestedLoop;
import edu.ucsd.forward.query.physical.SendPlanImplIdb;
import edu.ucsd.forward.query.physical.SendPlanImplInMemory;
import edu.ucsd.forward.query.physical.SendPlanImplJdbc;
import edu.ucsd.forward.query.physical.SendPlanImplRemote;
import edu.ucsd.forward.query.physical.SortImpl;
import edu.ucsd.forward.query.physical.SubqueryImpl;
import edu.ucsd.forward.query.physical.UnionImpl;
import edu.ucsd.forward.query.physical.ddl.CreateDataObjectImpl;
import edu.ucsd.forward.query.physical.ddl.DropDataObjectImpl;
import edu.ucsd.forward.query.physical.dml.AbstractDeleteImpl;
import edu.ucsd.forward.query.physical.dml.AbstractInsertImpl;
import edu.ucsd.forward.query.physical.dml.AbstractUpdateImpl;
import edu.ucsd.forward.query.physical.dml.AbstractUpdateImpl.AssignmentImpl;
import edu.ucsd.forward.query.physical.dml.DeleteImplIdb;
import edu.ucsd.forward.query.physical.dml.DeleteImplInMemory;
import edu.ucsd.forward.query.physical.dml.DeleteImplJdbc;
import edu.ucsd.forward.query.physical.dml.InsertImplIdb;
import edu.ucsd.forward.query.physical.dml.InsertImplInMemory;
import edu.ucsd.forward.query.physical.dml.InsertImplJdbc;
import edu.ucsd.forward.query.physical.dml.UpdateImplIdb;
import edu.ucsd.forward.query.physical.dml.UpdateImplInMemory;
import edu.ucsd.forward.query.physical.dml.UpdateImplJdbc;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * Builds a physical plan given a logical plan.
 * 
 * @author <Vicky Papavasileiou>
 * @author Yupeng
 */
public final class PhysicalPlanBuilder extends AbstractOperatorVisitor
{
    private UnifiedApplicationState           m_uas;
    
    private Stack<OperatorImpl>               m_stack_physical;
    
    /**
     * The context provided by the physical plan being created and the ones provided by outer physical plans.
     */
    private Stack<PhysicalPlanBuilderContext> m_contexts;
    
    /**
     * Hidden constructor.
     * 
     * @param contexts
     *            the context provided by the logical plan being created and the ones provided by outer physical plans.
     * @param uas
     *            the unified application state.
     */
    private PhysicalPlanBuilder(Stack<PhysicalPlanBuilderContext> contexts, UnifiedApplicationState uas)
    {
        assert (contexts != null);
        m_contexts = contexts;
        assert (uas != null);
        m_uas = uas;
        
        m_stack_physical = new Stack<OperatorImpl>();
    }
    
    /**
     * Builds a physical plan given a logical plan.
     * 
     * @param logical_plan
     *            the logical plan.
     * @param uas
     *            the unified application state.
     * 
     * @return a physical plan, that is, a tree of physical operators.
     */
    public static PhysicalPlan build(LogicalPlan logical_plan, UnifiedApplicationState uas)
    {
        return build(logical_plan, new Stack<PhysicalPlanBuilderContext>(), uas);
    }
    
    /**
     * Builds a physical plan given a logical plan.
     * 
     * @param logical_plan
     *            the logical plan.
     * @param contexts
     *            the context provided by the logical plan being created and the ones provided by outer physical plans.
     * @param uas
     *            the unified application state.
     * @return a physical plan, that is, a tree of physical operators.
     */
    private static PhysicalPlan build(LogicalPlan logical_plan, Stack<PhysicalPlanBuilderContext> contexts,
            UnifiedApplicationState uas)
    {
        PhysicalPlanBuilder builder = new PhysicalPlanBuilder(contexts, uas);
        
        builder.m_contexts.push(new PhysicalPlanBuilderContext(builder));
        
        List<AbstractAssignImpl> assign_impls = new ArrayList<AbstractAssignImpl>();
        
        // Visit the assign operators
        for (Assign assign : logical_plan.getAssigns())
        {
            assign.accept(builder);
            assign_impls.add((AbstractAssignImpl) builder.m_stack_physical.pop());
        }
        
        // Stack the logical operators for a bottom-up and right-to-left traversal
        Stack<Operator> stack_logical = new Stack<Operator>();
        LogicalPlanUtil.stackBottomUpRightToLeft(logical_plan.getRootOperator(), stack_logical);
        
        // Visit the logical operators bottom-up
        while (!stack_logical.isEmpty())
        {
            stack_logical.pop().accept(builder);
        }
        
        PhysicalPlan plan = new PhysicalPlan(builder.m_stack_physical.pop(), logical_plan);
        
        // Set the assign impls
        for (AbstractAssignImpl assign_impl : assign_impls)
        {
            plan.addAssignImpl(assign_impl);
        }
        
        if (!builder.m_stack_physical.empty())
        {
            assert (builder.m_stack_physical.empty());
        }
        
        return plan;
    }
    
    @Override
    public Operator visitAntiSemiJoin(AntiSemiJoin operator)
    {
        OperatorImpl op_impl = new AntiSemiJoinImplNestedLoop(operator, m_stack_physical.pop(), m_stack_physical.pop());
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitApplyPlan(ApplyPlan operator)
    {
        // Build the apply plan
        PhysicalPlan apply_plan = PhysicalPlanBuilder.build(operator.getLogicalPlansUsed().get(0), m_contexts, m_uas);
        
        ApplyPlanImpl op_impl = new ApplyPlanImpl(operator, m_stack_physical.pop());
        
        op_impl.setApplyPlan(apply_plan);
        
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitCopy(Copy operator)
    {
        // Build the copy plan
        PhysicalPlan copy_plan = PhysicalPlanBuilder.build(operator.getCopyPlan(), m_contexts, m_uas);
        
        String data_source_name = operator.getTargetDataSourceName();
        
        DataSource data_source = null;
        try
        {
            data_source = m_uas.getDataSource(data_source_name);
        }
        catch (DataSourceException e)
        {
            assert (false);
        }
        
        AbstractCopyImpl op_impl = null;
        switch (data_source.getMetaData().getStorageSystem())
        {
            case INMEMORY:
                op_impl = new CopyImplInMemory(operator);
                break;
            case JDBC:
                // FIXME No need to always bring the query result to the mediator if the target .
                op_impl = new CopyImplInMemory(operator);
                break;
            default:
                throw new AssertionError();
        }
        
        op_impl.setCopyPlan(copy_plan);
        
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitEliminateDuplicates(EliminateDuplicates operator)
    {
        OperatorImpl op_impl = new EliminateDuplicatesImpl(operator, m_stack_physical.pop());
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitExists(Exists operator)
    {
        // Build the exists plan
        PhysicalPlan exists_plan = PhysicalPlanBuilder.build(operator.getLogicalPlansUsed().get(0), m_contexts, m_uas);
        
        ExistsImpl op_impl = new ExistsImpl(operator, m_stack_physical.pop());
        
        op_impl.setExistsPlan(exists_plan);
        
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitGround(Ground operator)
    {
        OperatorImpl op_impl = new GroundImpl(operator);
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitGroupBy(GroupBy operator)
    {
        OperatorImpl op_impl = new GroupByImpl(operator, m_stack_physical.pop());
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitInnerJoin(InnerJoin operator)
    {
        OperatorImpl op_impl = new InnerJoinImplNestedLoop(operator, m_stack_physical.pop(), m_stack_physical.pop());
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitOffsetFetch(OffsetFetch operator)
    {
        OperatorImpl op_impl = new OffsetFetchImpl(operator, m_stack_physical.pop());
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitOuterJoin(OuterJoin operator)
    {
        OperatorImpl op_impl = new OuterJoinImplNestedLoop(operator, m_stack_physical.pop(), m_stack_physical.pop());
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitProduct(Product operator)
    {
        OperatorImpl op_impl = new ProductImplNestedLoop(operator, m_stack_physical.pop(), m_stack_physical.pop());
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitProject(Project operator)
    {
        OperatorImpl op_impl = new ProjectImpl(operator, m_stack_physical.pop());
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitScan(Scan operator)
    {
        if (operator.getTerm() instanceof AbsoluteVariable
                && ((AbsoluteVariable) operator.getTerm()).getDataSourceName().equals(DataSource.TEMP_ASSIGN_SOURCE))
        {
            String target = ((AbsoluteVariable) operator.getTerm()).getSchemaObjectName();
            AbstractAssignImpl assign_impl = getAssignImplByTarget(target);
            assert assign_impl != null;
            if (assign_impl instanceof AssignMemoryToMemoryImpl)
            {
                ScanImplReferencing opim = new ScanImplReferencing(operator, m_stack_physical.pop());
                opim.setReferencedAssign((AssignMemoryToMemoryImpl) assign_impl);
                m_stack_physical.push(opim);
                return operator;
            }
        }
        
        OperatorImpl op_impl = new ScanImpl(operator, m_stack_physical.pop());
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitSelect(Select operator)
    {
        OperatorImpl op_impl = new SelectImplSeq(operator, m_stack_physical.pop());
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitSemiJoin(SemiJoin operator)
    {
        OperatorImpl op_impl = new SemiJoinImplNestedLoop(operator, m_stack_physical.pop(), m_stack_physical.pop());
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitCreateSchemaObject(CreateDataObject operator)
    {
        CreateDataObjectImpl op_impl = new CreateDataObjectImpl(operator);
        // Build the plans
        for (SchemaPath path : operator.getTypesToSetDefault())
        {
            PhysicalPlan plan = build(operator.getDefaultPlan(path), m_contexts, m_uas);
            
            op_impl.addDefaultPlan(path, plan);
        }
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitDropSchemaObject(DropDataObject operator)
    {
        OperatorImpl op_impl = new DropDataObjectImpl(operator);
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitSendPlan(SendPlan operator)
    {
        // Build the send plan
        PhysicalPlan send_plan = build(operator.getSendPlan(), m_contexts, m_uas);
        
        String data_source_name = operator.getExecutionDataSourceName();
        
        DataSource data_source = null;
        try
        {
            data_source = m_uas.getDataSource(data_source_name);
        }
        catch (DataSourceException e)
        {
            assert (false);
        }
        
        AbstractSendPlanImpl op_impl = null;
        switch (data_source.getMetaData().getStorageSystem())
        {
            case INMEMORY:
                op_impl = new SendPlanImplInMemory(operator);
                break;
            case INDEXEDDB:
                op_impl = new SendPlanImplIdb(operator);
                break;
            case JDBC:
                op_impl = new SendPlanImplJdbc(operator);
                break;
            case REMOTE:
                op_impl = new SendPlanImplRemote(operator);
                break;
            default:
                throw new AssertionError();
        }
        
        for (@SuppressWarnings("unused")
        Operator child : operator.getChildren())
        {
            op_impl.addChild(m_stack_physical.pop());
        }
        
        op_impl.setSendPlan(send_plan);
        
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitSetOperator(SetOperator operator)
    {
        OperatorImpl op_impl = null;
        
        switch (operator.getSetOpType())
        {
            case UNION:
                op_impl = new UnionImpl(operator, m_stack_physical.pop(), m_stack_physical.pop());
                break;
            case INTERSECT:
                op_impl = new IntersectImpl(operator, m_stack_physical.pop(), m_stack_physical.pop());
                break;
            case EXCEPT:
                op_impl = new ExceptImpl(operator, m_stack_physical.pop(), m_stack_physical.pop());
                break;
            case OUTER_UNION:
                op_impl = new OuterUnionImpl(operator, m_stack_physical.pop(), m_stack_physical.pop());
                break;
            default:
                throw new AssertionError();
        }
        
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitSort(Sort operator)
    {
        OperatorImpl op_impl = new SortImpl(operator, m_stack_physical.pop());
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitInsert(Insert operator)
    {
        // Build the insert plan
        PhysicalPlan insert_plan = build(operator.getLogicalPlansUsed().get(0), m_contexts, m_uas);
        
        // Get the storage system of the target data source
        String target_data_source_name = operator.getTargetDataSourceName();
        DataSource target_data_source = null;
        try
        {
            target_data_source = m_uas.getDataSource(target_data_source_name);
        }
        catch (DataSourceException e)
        {
            assert (false);
        }
        
        AbstractInsertImpl op_impl = null;
        switch (target_data_source.getMetaData().getStorageSystem())
        {
            case INMEMORY:
                op_impl = new InsertImplInMemory(operator, m_stack_physical.pop());
                break;
            case JDBC:
                op_impl = new InsertImplJdbc(operator, m_stack_physical.pop());
                break;
            case INDEXEDDB:
                op_impl = new InsertImplIdb(operator, m_stack_physical.pop());
                break;
            default:
                throw new AssertionError();
        }
        
        op_impl.setInsertPlan(insert_plan);
        
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitNavigate(Navigate operator)
    {
        OperatorImpl op_impl = new NavigateImpl(operator, m_stack_physical.pop());
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitDelete(Delete operator)
    {
        // Get the storage system of the target data source
        String target_data_source_name = operator.getTargetDataSourceName();
        DataSource target_data_source = null;
        try
        {
            target_data_source = m_uas.getDataSource(target_data_source_name);
        }
        catch (DataSourceException e)
        {
            assert (false);
        }
        
        AbstractDeleteImpl op_impl = null;
        switch (target_data_source.getMetaData().getStorageSystem())
        {
            case INMEMORY:
                op_impl = new DeleteImplInMemory(operator, m_stack_physical.pop());
                break;
            case JDBC:
                op_impl = new DeleteImplJdbc(operator, m_stack_physical.pop());
                break;
            case INDEXEDDB:
                op_impl = new DeleteImplIdb(operator, m_stack_physical.pop());
                break;
            default:
                throw new AssertionError();
        }
        
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitUpdate(Update operator)
    {
        // Get the storage system of the target data source
        String target_data_source_name = operator.getTargetDataSourceName();
        DataSource target_data_source = null;
        try
        {
            target_data_source = m_uas.getDataSource(target_data_source_name);
        }
        catch (DataSourceException e)
        {
            assert (false);
        }
        
        AbstractUpdateImpl op_impl = null;
        switch (target_data_source.getMetaData().getStorageSystem())
        {
            case INMEMORY:
                op_impl = new UpdateImplInMemory(operator, m_stack_physical.pop());
                break;
            case JDBC:
                op_impl = new UpdateImplJdbc(operator, m_stack_physical.pop());
                break;
            case INDEXEDDB:
                op_impl = new UpdateImplIdb(operator, m_stack_physical.pop());
                break;
            default:
                throw new AssertionError();
        }
        
        // Build the assignment plans
        for (Assignment assignment : operator.getAssignments())
        {
            PhysicalPlan assignment_plan = build(assignment.getLogicalPlan(), m_contexts, m_uas);
            
            AssignmentImpl assignment_impl = op_impl.new AssignmentImpl(assignment, assignment_plan);
            
            op_impl.addAssignmentImpl(assignment_impl);
        }
        
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitPartitionBy(PartitionBy operator)
    {
        OperatorImpl op_impl = new PartitionByImpl(operator, m_stack_physical.pop());
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitIndexScan(IndexScan operator)
    {
        OperatorImpl op_impl = new IndexScanImpl(operator, m_stack_physical.pop());
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    @Override
    public Operator visitAssign(Assign operator)
    {
        // Build the assign plan
        PhysicalPlan assign_plan = build(operator.getLogicalPlansUsed().get(0), m_contexts, m_uas);
        
        String data_source_name = operator.getExecutionDataSourceName();
        
        DataSource data_source = null;
        try
        {
            data_source = m_uas.getDataSource(data_source_name);
        }
        catch (DataSourceException e)
        {
            assert (false);
        }
        
        AbstractAssignImpl op_impl = null;
        switch (data_source.getMetaData().getStorageSystem())
        {
            case INMEMORY:
                op_impl = new AssignMemoryToMemoryImpl(operator);
                op_impl.setAssignPlan(assign_plan);
                break;
            case JDBC:
                op_impl = new AssignImplJDBC(operator);
                op_impl.setAssignPlan(assign_plan);
                break;
            default:
                throw new AssertionError();
        }
        
        // Register the assign impl
        m_contexts.peek().addAssignTarget(operator.getTarget(), op_impl);
        
        m_stack_physical.push(op_impl);
        
        return operator;
    }
    
    /**
     * Gets the assign operator impl in all the contexts with respect to the target name.
     * 
     * @param target
     *            a specific assign target name.
     * @return the assign operator impl if found, <code>null</code> otherwise
     */
    private AbstractAssignImpl getAssignImplByTarget(String target)
    {
        for (PhysicalPlanBuilderContext context : m_contexts)
        {
            if (context.containsAssignTarget(target)) return context.getAssgin(target);
        }
        return null;
    }
    
    @Override
    public Operator visitSubquery(Subquery operator)
    {
        OperatorImpl op_impl = new SubqueryImpl(operator, m_stack_physical.pop());
        m_stack_physical.push(op_impl);
        
        return operator;
    }
}
