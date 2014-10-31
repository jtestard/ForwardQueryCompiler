/**
 * 
 */
package edu.ucsd.forward.query.logical.visitors;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.index.IndexDeclaration;
import edu.ucsd.forward.data.index.IndexUtil;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.DataSourceMetaData.StorageSystem;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.Function;
import edu.ucsd.forward.query.function.FunctionCall;
import edu.ucsd.forward.query.function.comparison.EqualFunction;
import edu.ucsd.forward.query.function.comparison.GreaterEqualFunction;
import edu.ucsd.forward.query.function.comparison.GreaterThanFunction;
import edu.ucsd.forward.query.function.comparison.LessEqualFunction;
import edu.ucsd.forward.query.function.comparison.LessThanFunction;
import edu.ucsd.forward.query.logical.AntiSemiJoin;
import edu.ucsd.forward.query.logical.ApplyPlan;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;
import edu.ucsd.forward.query.logical.Copy;
import edu.ucsd.forward.query.logical.EliminateDuplicates;
import edu.ucsd.forward.query.logical.Exists;
import edu.ucsd.forward.query.logical.Ground;
import edu.ucsd.forward.query.logical.GroupBy;
import edu.ucsd.forward.query.logical.IndexScan;
import edu.ucsd.forward.query.logical.IndexScan.KeyRangeSpec;
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
import edu.ucsd.forward.query.logical.term.AbsoluteVariable;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.logical.term.Term;

/**
 * The operator visitor that builds index scan operators.
 * 
 * @author Yupeng
 * 
 */
public final class IndexScanRewriter extends AbstractOperatorVisitor
{
    @SuppressWarnings("unused")
    private static final Logger     log = Logger.getLogger(IndexScanRewriter.class);
    
    private UnifiedApplicationState m_uas;
    
    /**
     * Hidden constructor.
     * 
     * @param uas
     *            the unified application state.
     */
    private IndexScanRewriter(UnifiedApplicationState uas)
    {
        assert uas != null;
        m_uas = uas;
    }
    
    /**
     * Gets a new index scan rewriter.
     * 
     * @param uas
     *            the unified application state.
     * @return an index scan rewriter instance.
     */
    public static IndexScanRewriter getInstance(UnifiedApplicationState uas)
    {
        return new IndexScanRewriter(uas);
    }
    
    /**
     * Visits the input logical plan, combines select operator and its child access path operator to index scan operator.
     * 
     * @param logical_plan
     *            the logical plan to visit.
     * @return the logical plan with index scan operator.
     */
    public LogicalPlan rewrite(LogicalPlan logical_plan)
    {
        Operator old_root = logical_plan.getRootOperator();
        Operator new_root = old_root.accept(this);
        
        for (Assign assign : logical_plan.getAssigns())
        {
            IndexScanRewriter.getInstance(m_uas).rewrite(assign.getLogicalPlansUsed().get(0));
        }
        
        if (old_root != new_root)
        {
            logical_plan.setRootOperator(new_root);
            logical_plan.updateOutputInfoShallow();
        }
        
        return logical_plan;
    }
    
    @Override
    public Operator visitAntiSemiJoin(AntiSemiJoin operator)
    {
        this.visitChildren(operator);
        return operator;
    }
    
    @Override
    public Operator visitApplyPlan(ApplyPlan operator)
    {
        IndexScanRewriter.getInstance(m_uas).rewrite(operator.getLogicalPlansUsed().get(0));
        this.visitChildren(operator);
        return operator;
    }
    
    @Override
    public Operator visitCopy(Copy operator)
    {
        // This should not be called since there shouldn't be any such operators at the time of this rewriting
        throw new AssertionError();
    }
    
    @Override
    public Operator visitCreateSchemaObject(CreateDataObject operator)
    {
        for (SchemaPath path : operator.getTypesToSetDefault())
        {
            LogicalPlan plan = operator.getDefaultPlan(path);
            IndexScanRewriter.getInstance(m_uas).rewrite(plan);
        }
        this.visitChildren(operator);
        return operator;
    }
    
    @Override
    public Operator visitDelete(Delete operator)
    {
        this.visitChildren(operator);
        return operator;
    }
    
    @Override
    public Operator visitDropSchemaObject(DropDataObject operator)
    {
        this.visitChildren(operator);
        return operator;
    }
    
    @Override
    public Operator visitEliminateDuplicates(EliminateDuplicates operator)
    {
        this.visitChildren(operator);
        return operator;
    }
    
    @Override
    public Operator visitExists(Exists operator)
    {
        IndexScanRewriter.getInstance(m_uas).rewrite(operator.getLogicalPlansUsed().get(0));
        this.visitChildren(operator);
        return operator;
    }
    
    @Override
    public Operator visitGround(Ground operator)
    {
        return operator;
    }
    
    @Override
    public Operator visitGroupBy(GroupBy operator)
    {
        this.visitChildren(operator);
        return operator;
    }
    
    @Override
    public Operator visitIndexScan(IndexScan operator)
    {
        // This should not be called since there shouldn't be any such operators at the time of this rewriting
        throw new AssertionError();
    }
    
    @Override
    public Operator visitInnerJoin(InnerJoin operator)
    {
        this.visitChildren(operator);
        return operator;
    }
    
    @Override
    public Operator visitInsert(Insert operator)
    {
        IndexScanRewriter.getInstance(m_uas).rewrite(operator.getLogicalPlansUsed().get(0));
        this.visitChildren(operator);
        return operator;
    }
    
    @Override
    public Operator visitNavigate(Navigate operator)
    {
        this.visitChildren(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitOffsetFetch(OffsetFetch operator)
    {
        this.visitChildren(operator);
        return operator;
    }
    
    @Override
    public Operator visitOuterJoin(OuterJoin operator)
    {
        this.visitChildren(operator);
        return operator;
    }
    
    @Override
    public Operator visitPartitionBy(PartitionBy operator)
    {
        this.visitChildren(operator);
        return operator;
    }
    
    @Override
    public Operator visitProduct(Product operator)
    {
        this.visitChildren(operator);
        return operator;
    }
    
    @Override
    public Operator visitProject(Project operator)
    {
        this.visitChildren(operator);
        return operator;
    }
    
    @Override
    public Operator visitScan(Scan operator)
    {
        this.visitChildren(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitSelect(Select operator)
    {
        if (!(operator.getChild() instanceof Scan))
        {
            this.visitChildren(operator);
            return operator;
        }
        
        Scan access_path = (Scan) operator.getChild();
        
        // Check the possibility of using index
        Term term = access_path.getTerm();
        if (!(term instanceof AbsoluteVariable))
        {
            this.visitChildren(operator);
            return operator;
        }
        AbsoluteVariable absolute_var = (AbsoluteVariable) term;
        
        String source_name = absolute_var.getDataSourceName();
        String data_obj_name = absolute_var.getSchemaObjectName();
        
        DataSource data_src;
        try
        {
            data_src = m_uas.getDataSource(source_name);
            // Currently only supports in-memory data source and indexedDB source
            if (!(data_src.getMetaData().getStorageSystem() == StorageSystem.INMEMORY || data_src.getMetaData().getStorageSystem() == StorageSystem.INDEXEDDB))
            {
                this.visitChildren(operator);
                return operator;
            }
            SchemaTree schema = data_src.getSchemaObject(data_obj_name).getSchemaTree();
            List<IndexDeclaration> declarations = IndexUtil.getIndexDeclarations(schema);
            
            IndexScan index_scan = buildIndexScan(operator, access_path, declarations);
            
            if (index_scan == null) return operator;
            
            if (operator.getConditions().isEmpty())
            {
                if (operator.getParent() != null)
                {
                    // Remove the select operator
                    Operator parent = operator.getParent();
                    parent.removeChild(operator);
                    parent.addChild(index_scan);
                }
                index_scan.updateOutputInfo();
                return index_scan;
            }
            else
            {
                operator.addChild(index_scan);
                operator.updateOutputInfo();
                return operator;
            }
        }
        catch (DataSourceException e)
        {
            // Should not happen
            throw new AssertionError();
        }
        catch (QueryExecutionException e)
        {
            // Should not happen
            throw new AssertionError();
        }
        catch (QueryCompilationException e)
        {
            // Should not happen
            throw new AssertionError();
        }
        
    }
    
    /**
     * Builds the index scan operator.
     * 
     * @param operator
     *            the select operator.
     * @param scan
     *            the scan operator under the select operator
     * @param declarations
     *            the index declarations
     * @return the built index scan operator.
     */
    private IndexScan buildIndexScan(Select operator, Scan scan, List<IndexDeclaration> declarations)
    {
        IndexScan index_scan = new IndexScan(scan.getAliasVariable().getName(), scan.getTerm(), "");
        KeyRangeSpec spec = new IndexScan.KeyRangeSpec();
        String index_name = null;
        
        for (IndexDeclaration declaration : declarations)
        {
            String key_step = declaration.getKeyPaths().get(0).getLastPathStep();
            boolean found = false;
            for (Term condition : new ArrayList<Term>(operator.getConditions()))
            {
                FunctionCall<?> func = (FunctionCall<?>) condition;
                if (!isKeyRangeFunction(func)) continue;
                
                // Check if the key path is at lhs
                if (checkLeft(key_step, func, spec))
                {
                    found = true;
                    operator.removeCondition(condition);
                    continue;
                }
                if (checkRight(key_step, func, spec))
                {
                    found = true;
                    operator.removeCondition(condition);
                }
            }
            // FIXME: Currently we only support one attribute as key
            if (found)
            {
                index_name = declaration.getName();
                break;
            }
        }
        
        if (index_name == null) return null;
        
        index_scan = new IndexScan(scan.getAliasVariable().getName(), scan.getTerm(), index_name);
        index_scan.addKeyRangeSpec(spec);
        // Move the child operator from access path to index scan
        Operator access_path_child = scan.getChild();
        scan.removeChild(access_path_child);
        index_scan.addChild(access_path_child);
        
        // Detach the access path operator
        operator.removeChild(scan);
        try
        {
            index_scan.updateOutputInfo();
        }
        catch (QueryCompilationException e)
        {
            assert false;
        }
        // Set size
        index_scan.setCardinalityEstimate(Size.SMALL);
        return index_scan;
    }
    
    private boolean checkLeft(String key_step, FunctionCall<?> func, KeyRangeSpec spec)
    {
        Term left = func.getArguments().get(0);
        if (!(left instanceof QueryPath)) return false;
        String query_path_step = ((QueryPath) left).getLastPathStep();
        if (!key_step.equals(query_path_step)) return false;
        if (func.getFunction() instanceof EqualFunction)
        {
            spec.setLowerOpen(false);
            spec.setUpperOpen(false);
            spec.setLowerTerm(func.getArguments().get(1));
            spec.setUpperTerm(func.getArguments().get(1));
        }
        else if (func.getFunction() instanceof LessEqualFunction)
        {
            spec.setUpperTerm(func.getArguments().get(1));
            spec.setUpperOpen(false);
        }
        else if (func.getFunction() instanceof LessThanFunction)
        {
            spec.setUpperTerm(func.getArguments().get(1));
            spec.setUpperOpen(true);
        }
        else if (func.getFunction() instanceof GreaterThanFunction)
        {
            spec.setLowerTerm(func.getArguments().get(1));
            spec.setLowerOpen(true);
        }
        else
        {
            assert (func.getFunction() instanceof GreaterEqualFunction);
            spec.setLowerTerm(func.getArguments().get(1));
            spec.setLowerOpen(false);
        }
        return true;
    }
    
    private boolean checkRight(String key_step, FunctionCall<?> func, KeyRangeSpec spec)
    {
        Term right = func.getArguments().get(1);
        if (!(right instanceof QueryPath)) return false;
        String query_path_step = ((QueryPath) right).getLastPathStep();
        if (!key_step.equals(query_path_step)) return false;
        if (func.getFunction() instanceof EqualFunction)
        {
            spec.setLowerOpen(false);
            spec.setUpperOpen(false);
            spec.setLowerTerm(func.getArguments().get(0));
            spec.setUpperTerm(func.getArguments().get(0));
        }
        else if (func.getFunction() instanceof LessEqualFunction)
        {
            spec.setLowerTerm(func.getArguments().get(0));
            spec.setLowerOpen(false);
        }
        else if (func.getFunction() instanceof LessThanFunction)
        {
            spec.setLowerTerm(func.getArguments().get(0));
            spec.setLowerOpen(true);
        }
        else if (func.getFunction() instanceof GreaterThanFunction)
        {
            spec.setUpperTerm(func.getArguments().get(0));
            spec.setUpperOpen(true);
        }
        else if (func.getFunction() instanceof GreaterEqualFunction)
        {
            spec.setUpperTerm(func.getArguments().get(0));
            spec.setUpperOpen(false);
        }
        return true;
    }
    
    /**
     * Checks if the given function call is key range function call.
     * 
     * @param call
     *            the given function call.
     * @return <code>true</code> if the given function call is key range function call; <code>false</code> otherwise.
     */
    private boolean isKeyRangeFunction(FunctionCall<?> call)
    {
        Function func = call.getFunction();
        if (func instanceof EqualFunction || func instanceof LessEqualFunction || func instanceof LessThanFunction
                || func instanceof GreaterEqualFunction || func instanceof GreaterThanFunction) return true;
        return false;
    }
    
    @Override
    public Operator visitSemiJoin(SemiJoin operator)
    {
        this.visitChildren(operator);
        return operator;
    }
    
    @Override
    public Operator visitSendPlan(SendPlan operator)
    {
        // This should not be called since there shouldn't be any such operators at the time of this rewriting
        throw new AssertionError();
    }
    
    @Override
    public Operator visitSetOperator(SetOperator operator)
    {
        this.visitChildren(operator);
        return operator;
    }
    
    @Override
    public Operator visitSort(Sort operator)
    {
        this.visitChildren(operator);
        return operator;
    }
    
    @Override
    public Operator visitUpdate(Update operator)
    {
        for (Assignment assignment : operator.getAssignments())
        {
            IndexScanRewriter.getInstance(m_uas).rewrite(assignment.getLogicalPlan());
        }
        
        this.visitChildren(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitAssign(Assign operator)
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Operator visitSubquery(Subquery subquery)
    {
        // FIXME
        throw new UnsupportedOperationException();
    }
}
