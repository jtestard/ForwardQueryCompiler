/**
 * 
 */
package edu.ucsd.forward.query.logical.visitors;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.constraint.LocalPrimaryKeyConstraint;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.JsonType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.SetOpExpression.SetOpType;
import edu.ucsd.forward.query.ast.SetQuantifier;
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
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.PartitionBy;
import edu.ucsd.forward.query.logical.Product;
import edu.ucsd.forward.query.logical.Project;
import edu.ucsd.forward.query.logical.Subquery;
import edu.ucsd.forward.query.logical.Project.Item;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.Select;
import edu.ucsd.forward.query.logical.SemiJoin;
import edu.ucsd.forward.query.logical.SendPlan;
import edu.ucsd.forward.query.logical.SetOperator;
import edu.ucsd.forward.query.logical.Sort;
import edu.ucsd.forward.query.logical.UnaryOperator;
import edu.ucsd.forward.query.logical.ddl.CreateDataObject;
import edu.ucsd.forward.query.logical.ddl.DropDataObject;
import edu.ucsd.forward.query.logical.dml.Delete;
import edu.ucsd.forward.query.logical.dml.Insert;
import edu.ucsd.forward.query.logical.dml.Update;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.plan.LogicalPlanUtil;
import edu.ucsd.forward.query.logical.term.Constant;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.util.NameGenerator;
import edu.ucsd.forward.util.NameGeneratorFactory;

/**
 * The logical plan visitor that infers keys for each operator in a logical plan. If a key cannot be found, this builder will
 * attempt to change the plan in place. The keys are represented as a set of relative variables in the OutputInfo, which is based on
 * the assumption that all the keys are direct attributes in the output type.
 * 
 * This builder operates on the new scan, and hence it should be invoked after the ConsolidateNavigates rewriting.
 * 
 * @author Yupeng
 * 
 */
public final class KeyInferenceBuilder extends AbstractOperatorVisitor
{
    @SuppressWarnings("unused")
    private static final Logger log           = Logger.getLogger(KeyInferenceBuilder.class);
    
    /**
     * Indicates if the key inference is performed incrementally.
     */
    private boolean             m_incremental = false;
    
    private LogicalPlan         m_plan;
    
    /**
     * Hidden constructor.
     * 
     */
    private KeyInferenceBuilder(LogicalPlan plan, boolean incremental)
    {
        assert plan != null;
        m_plan = plan;
        m_incremental = incremental;
    }
    
    /**
     * Infer the keys for the given logical plan in place. Warning: The input plan might be modified.
     * 
     */
    private void build()
    {
        assert m_plan != null;
        m_plan.clearEquivalentGroups();
        m_plan.getRootOperator().accept(this);
        
        for (Assign assign : m_plan.getAssigns())
        {
            assign.accept(this);
        }
    }
    
    /**
     * Infer the keys for the given operator in place. Warning: The input plan might be modified.
     * 
     * @param operator
     *            the starting operator.
     */
    private void build(Operator operator)
    {
        assert m_plan != null;
        operator.accept(this);
    }
    
    /**
     * Recursively infers the key information for all the operators within the plan.
     * 
     * @param plan
     *            the plan to have key inferred for each operator.
     */
    public static void build(LogicalPlan plan)
    {
        KeyInferenceBuilder builder = new KeyInferenceBuilder(plan, false);
        builder.build();
    }
    
    /**
     * Recursively infers the key information of the subtree rooting the given operator.
     * 
     * @param operator
     *            the operator.
     * @param plan
     *            the plan to have key inferred for each operator.
     */
    public static void build(Operator operator, LogicalPlan plan)
    {
        KeyInferenceBuilder builder = new KeyInferenceBuilder(plan, false);
        builder.build(operator);
    }
    
    /**
     * Recursively updates the key annotation of all the ancestor operators of the input operator.
     * 
     * @param operator
     *            the current operator being visited.
     * @param plan
     *            the belonging plan.
     */
    public static void buildAncestor(Operator operator, LogicalPlan plan)
    {
        KeyInferenceBuilder builder = new KeyInferenceBuilder(plan, true);
        Operator op = operator;
        
        if (op.getParent() != null)
        {
            builder.build(op);
        }
    }
    
    /**
     * Incrementally infers the key information of the given operator.The caller of this method must be cautious to guarantee the
     * validity of the key annotation of the subtree as well as the key equivalency information stored in the logical plan.
     * 
     * @param operator
     *            the operator.
     * @param plan
     *            the plan to have key inferred for each operator.
     */
    public static void buildIncrementally(Operator operator, LogicalPlan plan)
    {
        KeyInferenceBuilder builder = new KeyInferenceBuilder(plan, true);
        builder.build(operator);
    }
    
    @Override
    protected void visitChildren(Operator operator)
    {
        // Do not visit children if the inference is performed incrementally.
        if (m_incremental) return;
        super.visitChildren(operator);
        
        // Update the output info, as the descendents might be retouched.
        try
        {
            operator.updateOutputInfo();
        }
        catch (QueryCompilationException e)
        {
            assert false : e.getMessage();
        }
    }
    
    private void visitChildrenWithoutUpdatingSelf(Operator operator)
    {
        // Do not visit children if the inference is performed incrementally.
        if (m_incremental) return;
        super.visitChildren(operator);
    }
    
    @Override
    public Operator visitAntiSemiJoin(AntiSemiJoin operator)
    {
        visitChildren(operator);
        OutputInfo child_output = operator.getChildren().get(0).getOutputInfo();
        operator.getOutputInfo().setKeyVars(child_output.getKeyVars());
        return operator;
    }
    
    @Override
    public Operator visitApplyPlan(ApplyPlan operator)
    {
        visitChildren(operator);
        if (!m_incremental)
        {
            build(operator.getApplyPlan());
            try
            {
                operator.updateOutputInfo();
            }
            catch (QueryCompilationException e)
            {
                assert false;
            }
        }
        // propagate the input key.
        propagate(operator);
        return operator;
    }
    
    @Override
    public Operator visitCopy(Copy operator)
    {
        // Shouldn't infer key of this physical operator.
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Operator visitCreateSchemaObject(CreateDataObject operator)
    {
        // Nothing to do
        return operator;
    }
    
    @Override
    public Operator visitDelete(Delete operator)
    {
        visitChildren(operator);
        // Nothing to infer
        return operator;
    }
    
    @Override
    public Operator visitDropSchemaObject(DropDataObject operator)
    {
        return operator;
    }
    
    @Override
    public Operator visitEliminateDuplicates(EliminateDuplicates operator)
    {
        visitChildren(operator);
        Set<RelativeVariable> input_keys = new LinkedHashSet<RelativeVariable>(operator.getChild().getOutputInfo().getKeyVars());
        // The keys that are preserved candidates of input ones.
        Set<RelativeVariable> candidate_keys = new LinkedHashSet<RelativeVariable>();
        // The list of group by terms as keys.
        Set<RelativeVariable> full_keys = new LinkedHashSet<RelativeVariable>();
        for (RelativeVariable out_var : operator.getOutputInfo().getVariables())
        {
            if (!(out_var.getType() instanceof ScalarType)) continue;
            for (RelativeVariable in_key : new LinkedHashSet<RelativeVariable>(input_keys))
            {
                // The output term is the input key or its equivalent
                if (in_key.equals(out_var) || m_plan.isEquivalent(in_key, out_var))
                {
                    input_keys.remove(in_key);
                    candidate_keys.add(out_var);
                    break;
                }
            }
            
            full_keys.add(out_var);
        }
        // If the group by terms subsume all the input keys, then use the input keys only.
        if (input_keys.isEmpty())
        {
            operator.getOutputInfo().setKeyVars(candidate_keys);
        }
        else
        {
            operator.getOutputInfo().setKeyVars(full_keys);
        }
        return operator;
    }
    
    @Override
    public Operator visitExists(Exists operator)
    {
        visitChildren(operator);
        propagate(operator);
        return operator;
    }
    
    @Override
    public Operator visitGround(Ground operator)
    {
        // Do nothing.
        return operator;
    }
    
    @Override
    public Operator visitGroupBy(GroupBy operator)
    {
        visitChildren(operator);
        Set<RelativeVariable> input_keys = new LinkedHashSet<RelativeVariable>(operator.getChild().getOutputInfo().getKeyVars());
        // The keys that are preserved candidates of input ones.
        Set<RelativeVariable> candidate_keys = new LinkedHashSet<RelativeVariable>();
        // The list of group by terms as keys.
        Set<RelativeVariable> full_keys = new LinkedHashSet<RelativeVariable>();
        for (Term group_by_term : operator.getGroupByTerms())
        {
            if (!(group_by_term instanceof RelativeVariable)) continue;
            RelativeVariable group_term = (RelativeVariable) group_by_term;
            if (!(group_term.getType() instanceof ScalarType)) continue;
            for (RelativeVariable in_key : new LinkedHashSet<RelativeVariable>(input_keys))
            {
                // The group by term is the input key or its equivalent
                if (in_key.equals(group_term) || m_plan.isEquivalent(in_key, group_term))
                {
                    input_keys.remove(in_key);
                    candidate_keys.add(group_term);
                    break;
                }
            }
            
            full_keys.add(group_term);
        }
        // If the group by terms subsume all the input keys, then use the input keys only.
        if (input_keys.isEmpty())
        {
            operator.getOutputInfo().setKeyVars(candidate_keys);
        }
        else
        {
            operator.getOutputInfo().setKeyVars(full_keys);
        }
        
        return operator;
    }
    
    @Override
    public Operator visitIndexScan(IndexScan operator)
    {
        return visitScan(operator);
    }
    
    @Override
    public Operator visitInnerJoin(InnerJoin operator)
    {
        visitChildren(operator);
        inferJoin(operator);
        return operator;
    }
    
    @Override
    public Operator visitInsert(Insert operator)
    {
        visitChildren(operator);
        // Nothing to infer
        return operator;
    }
    
    @Override
    public Operator visitNavigate(Navigate operator)
    {
        // Nothing to infer
        this.visitChildren(operator);
        propagate(operator);
        return operator;
    }
    
    @Override
    public Operator visitOffsetFetch(OffsetFetch operator)
    {
        visitChildren(operator);
        propagate(operator);
        return operator;
    }
    
    @Override
    public Operator visitOuterJoin(OuterJoin operator)
    {
        visitChildren(operator);
        inferJoin(operator);
        return operator;
    }
    
    @Override
    public Operator visitPartitionBy(PartitionBy operator)
    {
        this.visitChildren(operator);
        if (!operator.hasRankFunction())
        {
            propagate(operator);
        }
        else
        {
            // The keys of input.
            Set<RelativeVariable> in_keys = new LinkedHashSet<RelativeVariable>(operator.getChild().getOutputInfo().getKeyVars());
            // The new key is the combination of the partition-by attributes and the rank attribute
            Set<RelativeVariable> keys = new LinkedHashSet<RelativeVariable>();
            for (RelativeVariable term : operator.getPartitionByTerms())
            {
                keys.add(term);
                if (in_keys.contains(term)) in_keys.remove(term);
            }
            keys.add(operator.getOutputInfo().getVariable(operator.getRankAlias()));
            
            if (in_keys.isEmpty())
            {
                // If the group by terms subsume all the input keys, then use the input keys only.
                operator.getOutputInfo().setKeyVars(new LinkedHashSet<RelativeVariable>(
                                                                                        operator.getChild().getOutputInfo().getKeyVars()));
            }
            else
            {
                operator.getOutputInfo().setKeyVars(keys);
            }
        }
        return operator;
    }
    
    @Override
    public Operator visitProduct(Product operator)
    {
        visitChildren(operator);
        inferJoin(operator);
        return operator;
    }
    
    @Override
    public Operator visitProject(Project operator)
    {
        visitChildren(operator);
        // A mapping between the keys of the input and the names of the variables in the corresponding equivalent groups
        Map<RelativeVariable, Set<String>> inkey_groups_map = new LinkedHashMap<RelativeVariable, Set<String>>();
        for (RelativeVariable in_key : operator.getChild().getOutputInfo().getKeyVars())
        {
            // Find equivalent groups as project items' alias
            Set<String> equiv_keys = new LinkedHashSet<String>();
            for (Item item : operator.getProjectionItems())
                if (item.getTerm().equals(in_key)) equiv_keys.add(item.getAlias());
            
            if (equiv_keys.isEmpty())
            {
                // Retouch the plan
                String new_key_name = getUniqueName();
                try
                {
                    operator.addProjectionItem(in_key, new_key_name, true);
                }
                catch (QueryCompilationException e)
                {
                    assert false;
                }
                equiv_keys.add(new_key_name);
            }
            
            inkey_groups_map.put(in_key, equiv_keys);
        }
        // Update output info to reflect the changes from retouching.
        try
        {
            operator.updateOutputInfo();
        }
        catch (QueryCompilationException e)
        {
            assert false;
        }
        
        Set<RelativeVariable> keys = new LinkedHashSet<RelativeVariable>();
        for (RelativeVariable in_key : inkey_groups_map.keySet())
        {
            // Register equivalent groups
            boolean first = true;
            for (String out_key : inkey_groups_map.get(in_key))
            {
                RelativeVariable out_var = operator.getOutputInfo().getVariable(out_key);
                if (first)
                {
                    keys.add(out_var);
                    first = false;
                }
                m_plan.addEquivalentKeys(in_key, out_var);
            }
        }
        
        operator.getOutputInfo().setKeyVars(keys);
        return operator;
    }
    
    @Override
    public Operator visitScan(Scan operator)
    {
        this.visitChildren(operator);
        try
        {
            inferScan(operator);
        }
        catch (QueryCompilationException e)
        {
            assert false;
        }
        return operator;
    }
    
    @Override
    public Operator visitSelect(Select operator)
    {
        this.visitChildren(operator);
        propagate(operator);
        return operator;
    }
    
    @Override
    public Operator visitSemiJoin(SemiJoin operator)
    {
        visitChildren(operator);
        OutputInfo child_output = operator.getChildren().get(0).getOutputInfo();
        operator.getOutputInfo().setKeyVars(child_output.getKeyVars());
        return operator;
    }
    
    @Override
    public Operator visitSendPlan(SendPlan operator)
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Operator visitSetOperator(SetOperator operator)
    {
        if (operator.getSetOpType() == SetOpType.UNION)
        {
            return visitUnion(operator);
        }
        else if (operator.getSetOpType() == SetOpType.INTERSECT)
        {
            return visitIntersect(operator);
        }
        else if (operator.getSetOpType() == SetOpType.EXCEPT)
        {
            return visitExcept(operator);
        }
        else if (operator.getSetOpType() == SetOpType.OUTER_UNION)
        {
            return visitUnion(operator);
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }
    
    /**
     * Checks if the output variables of the given operator contain retouched attributes.
     * 
     * @param operator
     *            the operator to chechk
     * @return if the output variables of the given operator contain retouched attributes.
     */
    private boolean hasRetouchedVariables(Operator operator)
    {
        for (RelativeVariable variable : operator.getOutputInfo().getVariables())
        {
            if (isRetouchedVariable(variable.getName()))
            {
                return true;
            }
        }
        return false;
    }
    
    private Operator visitUnion(SetOperator operator)
    {
        visitChildrenWithoutUpdatingSelf(operator);
        if (operator.getSetQuantifier() == SetQuantifier.DISTINCT)
        {
            rewriteAllChildrenToSet(operator);
        }
        else
        {
            // Change the set type to outer union
            operator.setSetOpType(SetOpType.OUTER_UNION);
            
            Operator left_child = operator.getChildren().get(0);
            Operator right_child = operator.getChildren().get(1);
            // Add branch index if neither child has retouched attributes (i.e. both are set)
            if (!hasRetouchedVariables(left_child) && !hasRetouchedVariables(right_child))
            {
                int index = 0;
                String alias = getUniqueName();
                for (Operator child_op : new ArrayList<Operator>(operator.getChildren()))
                {
                    index++;
                    Constant constant = new Constant(new IntegerValue(index));
                    try
                    {
                        if (child_op instanceof Project)
                        {
                            ((Project) child_op).addProjectionItem(constant, alias, true);
                        }
                        else
                        {
                            Project project = new Project();
                            replaceSetOpChild(operator, child_op, project);
                            project.addChild(child_op);
                            // Project all the variables from input
                            for (RelativeVariable variable : child_op.getOutputInfo().getVariables())
                            {
                                // String fresh_alias =
                                // NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.VARIABLE_GENERATOR);
                                // RelativeVariable new_var = new RelativeVariable(fresh_alias, variable.getDefaultProjectAlias());
                                // new_var.setType(variable.getType());
                                project.addProjectionItem(variable.copy(), variable.getDefaultProjectAlias(), true);
                            }
                            project.addProjectionItem(constant, alias, true);
                        }
                    }
                    catch (QueryCompilationException e)
                    {
                        // should never happen
                        assert false;
                    }
                }
            }
        }
        
        visitChildren(operator);
        
        // The output key is the union of input ones.
        Set<RelativeVariable> keys = new LinkedHashSet<RelativeVariable>();
        for (Operator child : operator.getChildren())
        {
            keys.addAll(child.getOutputInfo().getKeyVars());
            // Add the missing retouched attributes (branch index)
            for (RelativeVariable variable : child.getOutputInfo().getVariables())
            {
                if (isRetouchedVariable(variable.getName()) && !keys.contains(variable)) keys.add(variable);
            }
        }
        operator.getOutputInfo().setKeyVars(keys);
        return operator;
    }
    
    /**
     * Rewrite all the child operators of the given set operator to set semantic, by placing a distinct operator on top of it.
     * 
     * @param operator
     *            the set operator.
     */
    private void rewriteAllChildrenToSet(SetOperator operator)
    {
        for (Operator child_op : new ArrayList<Operator>(operator.getChildren()))
        {
            if (hasRetouchedVariables(child_op))
            {
                // Add a distinct operator on top of the child op to change its output to a set
                EliminateDuplicates eli = new EliminateDuplicates();
                replaceSetOpChild(operator, child_op, eli);
                eli.addChild(child_op);
            }
        }
    }
    
    private Operator visitIntersect(SetOperator operator)
    {
        visitChildrenWithoutUpdatingSelf(operator);
        
        Operator left_child = operator.getChildren().get(0);
        Operator right_child = operator.getChildren().get(1);
        if (operator.getSetQuantifier() == SetQuantifier.ALL && hasRetouchedVariables(left_child)
                && hasRetouchedVariables(right_child)) // Both branches are bag
        {
            addPartitionByOnTopOfBothChildren(operator);
            
            // The output key is all the attribute in left output
            Set<RelativeVariable> keys = operator.getChildren().get(0).getOutputInfo().getVariables();
            operator.getOutputInfo().setKeyVars(new LinkedHashSet<RelativeVariable>(keys));
            return operator;
        }
        
        rewriteAllChildrenToSet(operator);
        visitChildren(operator);
        
        // The output key is the smaller one of input.
        Set<RelativeVariable> left_keys = operator.getChildren().get(0).getOutputInfo().getKeyVars();
        Set<RelativeVariable> right_keys = operator.getChildren().get(1).getOutputInfo().getKeyVars();
        
        Set<RelativeVariable> keys = left_keys.size() < right_keys.size() ? left_keys : right_keys;
        operator.getOutputInfo().setKeyVars(new LinkedHashSet<RelativeVariable>(keys));
        return operator;
    }
    
    private boolean hasDoneAddingPartition(SetOperator operator)
    {
        Operator left_child = operator.getChildren().get(0);
        Operator right_child = operator.getChildren().get(1);
        
        if (!(left_child instanceof PartitionBy) || !(right_child instanceof PartitionBy)) return false;
        String left_rank = ((PartitionBy) left_child).getRankAlias();
        String right_rank = ((PartitionBy) right_child).getRankAlias();
        
        if (left_rank == null || right_rank == null) return false;
        return left_rank.equals(right_rank);
    }
    
    private void addPartitionByOnTopOfBothChildren(SetOperator operator)
    {
        String rank_index = getUniqueName();
        
        if (!hasDoneAddingPartition(operator)) // Do rewriting on demand
        {
            // Add a partition operator on each child
            for (Operator child_op : new ArrayList<Operator>(operator.getChildren()))
            {
                PartitionBy partition = new PartitionBy();
                replaceSetOpChild(operator, child_op, partition);
                partition.addChild(child_op);
                
                // Configure the child's output attributes as partitioning terms, and a rank function
                partition.setRankAlias(rank_index);
                for (RelativeVariable variable : child_op.getOutputInfo().getVariables())
                {
                    if (!isRetouchedVariable(variable.getName()))
                    {
                        partition.addPartitionByTerm((RelativeVariable) variable.copy());
                    }
                }
            }
        }
        visitChildren(operator);
    }
    
    private Operator visitExcept(SetOperator operator)
    {
        visitChildrenWithoutUpdatingSelf(operator);
        Operator left_child = operator.getChildren().get(0);
        if (operator.getSetQuantifier() == SetQuantifier.ALL && hasRetouchedVariables(left_child)) // Left branch is bag
        {
            addPartitionByOnTopOfBothChildren(operator);
            
            // The output key is all the attribute in left output
            Set<RelativeVariable> keys = operator.getChildren().get(0).getOutputInfo().getKeyVars();
            operator.getOutputInfo().setKeyVars(new LinkedHashSet<RelativeVariable>(keys));
            return operator;
        }
        
        // The left branch is a set
        rewriteAllChildrenToSet(operator);
        visitChildren(operator);
        
        // The output key is the key of left input.
        Set<RelativeVariable> keys = operator.getChildren().get(0).getOutputInfo().getKeyVars();
        operator.getOutputInfo().setKeyVars(new LinkedHashSet<RelativeVariable>(keys));
        return operator;
    }
    
    private void replaceSetOpChild(SetOperator operator, Operator old_child, Operator new_child)
    {
        if (old_child == operator.getChildren().get(0)) operator.replaceLeftChild(new_child);
        else operator.replaceRightChild(new_child);
    }
    
    @Override
    public Operator visitSort(Sort operator)
    {
        this.visitChildren(operator);
        propagate(operator);
        return operator;
    }
    
    @Override
    public Operator visitUpdate(Update operator)
    {
        visitChildren(operator);
        // Nothing to infer
        return operator;
    }
    
    /**
     * Propagates the key information from the child operator to the given unary operator. It's the caller's responsibility to
     * validate the output info of the child operator.
     * 
     * @param operator
     *            the given operator.
     */
    private void propagate(UnaryOperator operator)
    {
        // This operator itself has no change -- just copy the keys from child operator.
        OutputInfo child_output = operator.getChild().getOutputInfo();
        operator.getOutputInfo().setKeyVars(child_output.getKeyVars());
    }
    
    private void inferJoin(Operator operator)
    {
        try
        {
            operator.updateOutputInfo();
        }
        catch (QueryCompilationException e)
        {
            assert false;
        }
        // The keys are the combinations of the children operators'
        Set<RelativeVariable> keys = new LinkedHashSet<RelativeVariable>();
        for (Operator child : operator.getChildren())
        {
            keys.addAll(child.getOutputInfo().getKeyVars());
        }
        operator.getOutputInfo().setKeyVars(keys);
    }
    
    /**
     * Infers the keys for the scan operator.
     * 
     * @param scan
     *            the sacn operator
     * @throws QueryCompilationException
     */
    private void inferScan(Scan scan) throws QueryCompilationException
    {
        // Get the keys of the child operator
        Set<RelativeVariable> child_keys = scan.getChild().getOutputInfo().getKeyVars();
        
        // New keys
        Set<RelativeVariable> keys = new LinkedHashSet<RelativeVariable>();
        keys.addAll(child_keys);
        
        Term term = scan.getTerm();
        
        Type term_type = term.getType();
        
        if (term_type instanceof CollectionType)
        {
            // Get the primary keys types
            LocalPrimaryKeyConstraint constraint = ((CollectionType) term_type).getLocalPrimaryKeyConstraint();
            assert constraint != null;
            
            // Check all the keys have corresponding navigation variables.
            for (ScalarType key_type : constraint.getAttributes())
            {
                // Retouch the operator, add the navigate variable.
                SchemaPath sp = new SchemaPath(term_type, key_type);
                Navigate nav = new Navigate(getUniqueName(), new QueryPath(scan.getAliasVariable(), sp.relative(1).getPathSteps()));
                LogicalPlanUtil.insertOnTop(scan, nav);
                keys.add(nav.getAliasVariable());
            }
        }
        else if (term_type instanceof JsonType)
        {
            // TODO: read the json key from input source
            throw new UnsupportedOperationException();
        }
        else
        {
            assert term_type instanceof TupleType;
            // There is no key for the scanned item
        }
        
        // Merge the keys from scanned term and the input child
        scan.updateOutputInfo();
        scan.getOutputInfo().setKeyVars(keys);
    }
    
    /**
     * Tests if the given relative variable name is generated for key inference.
     * 
     * @param name
     *            the name to test.
     * @return if the given relative variable name is generated for key inference.
     */
    public static boolean isRetouchedVariable(String name)
    {
        return name.startsWith(NameGenerator.NAME_PREFIX + NameGenerator.KEY_VARIABLE_GENERATOR);
    }
    
    public static String getUniqueName()
    {
        return NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.KEY_VARIABLE_GENERATOR);
    }
    
    @Override
    public Operator visitAssign(Assign operator)
    {
        build(operator.getPlan());
        return operator;
    }
    
    @Override
    public Operator visitSubquery(Subquery subquery)
    {
        // FIXME
        throw new UnsupportedOperationException();
    }
}
