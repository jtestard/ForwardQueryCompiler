package edu.ucsd.forward.query.logical.visitors;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.NoSqlType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.query.ast.SetOpExpression.SetOpType;
import edu.ucsd.forward.query.function.FunctionCall;
import edu.ucsd.forward.query.logical.AntiSemiJoin;
import edu.ucsd.forward.query.logical.ApplyPlan;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.logical.Copy;
import edu.ucsd.forward.query.logical.EliminateDuplicates;
import edu.ucsd.forward.query.logical.Exists;
import edu.ucsd.forward.query.logical.Ground;
import edu.ucsd.forward.query.logical.GroupBy;
import edu.ucsd.forward.query.logical.Subquery;
import edu.ucsd.forward.query.logical.UnaryOperator;
import edu.ucsd.forward.query.logical.GroupBy.Aggregate;
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
import edu.ucsd.forward.query.logical.Project.Item;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.Select;
import edu.ucsd.forward.query.logical.SemiJoin;
import edu.ucsd.forward.query.logical.SendPlan;
import edu.ucsd.forward.query.logical.SetOperator;
import edu.ucsd.forward.query.logical.Sort;
import edu.ucsd.forward.query.logical.ddl.AbstractDdlOperator;
import edu.ucsd.forward.query.logical.ddl.CreateDataObject;
import edu.ucsd.forward.query.logical.ddl.DropDataObject;
import edu.ucsd.forward.query.logical.dml.AbstractDmlOperator;
import edu.ucsd.forward.query.logical.dml.Delete;
import edu.ucsd.forward.query.logical.dml.Insert;
import edu.ucsd.forward.query.logical.dml.Update;
import edu.ucsd.forward.query.logical.dml.Update.Assignment;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.plan.LogicalPlanUtil;
import edu.ucsd.forward.query.logical.term.AbsoluteVariable;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;

/**
 * The operator visitor that checks that a LogicalPlan matches the initial normal form (normal for after initial translation by the
 * LogicalPlanBuilder. It makes sure that the relative order between operators is respected, that each operator has the expected
 * number of child operators, that all the arguments of the operator are valid, and that all the used variables are in scope.
 * 
 * @author Romain Vernoux
 */
public final class InitialNormalFormChecker extends AbstractOperatorVisitor
{
    private UnifiedApplicationState m_uas;
    
    /**
     * Constructor.
     * 
     * @param uas
     *            the unified application state.
     */
    public InitialNormalFormChecker(UnifiedApplicationState uas)
    {
        assert (uas != null);
        m_uas = uas;
    }
    
    /**
     * Checks that the operators in the plan occur in a correct order.
     * 
     * @param root
     *            the root of the plan to check
     */
    private void checkOperatorOrder(Operator root)
    {
        Operator op = root;
        if (op instanceof AbstractDdlOperator)
        {
            // No normal form for DDL yet.
            return;
        }
        if (op instanceof AbstractDmlOperator)
        {
            // No normal form for DML yet.
            return;
        }
        if (op instanceof OffsetFetch)
        {
            op = LogicalPlanUtil.getNextClauseOperator(root);
        }
        if (op instanceof Sort)
        {
            op = LogicalPlanUtil.getNextClauseOperator(op);
            assert (op instanceof SetOperator);
        }
        checkOperatorOrderSetOperator(op);
    }
    
    /**
     * Checks that the operators in the plan occur in a correct order.
     * 
     * @param root
     *            the root of the plan to check
     */
    private void checkOperatorOrderSetOperator(Operator root)
    {
        if (root instanceof SetOperator)
        {
            for (Operator child : root.getChildren())
            {
                checkOperatorOrderSetOperator(child);
            }
        }
        else
        {
            checkOperatorOrderQuery(root);
        }
    }
    
    /**
     * Checks that the operators in the plan occur in a correct order.
     * 
     * @param root
     *            the root of the plan to check
     */
    private void checkOperatorOrderQuery(Operator root)
    {
        Operator op = root;
        if (op instanceof EliminateDuplicates)
        {
            op = LogicalPlanUtil.getNextClauseOperator(op);
        }
        assert (op instanceof Project);
        op = LogicalPlanUtil.getNextClauseOperator(op);
        if (op instanceof OffsetFetch)
        {
            op = LogicalPlanUtil.getNextClauseOperator(op);
        }
        if (op instanceof Sort)
        {
            op = LogicalPlanUtil.getNextClauseOperator(op);
        }
        if (op instanceof Select)
        {
            op = LogicalPlanUtil.getNextClauseOperator(op);
        }
        if (op instanceof GroupBy)
        {
            op = LogicalPlanUtil.getNextClauseOperator(op);
        }
        if (op instanceof Select)
        {
            op = LogicalPlanUtil.getNextClauseOperator(op);
        }
        if (op instanceof Product)
        {
            for (Operator child : op.getChildren())
            {
                checkOperatorOrderJoin(child);
            }
        }
        else
        {
            checkOperatorOrderJoin(op);
        }
    }
    
    /**
     * Checks that the operators in the plan occur in a correct order.
     * 
     * @param root
     *            the root of the plan to check
     */
    private void checkOperatorOrderJoin(Operator root)
    {
        Operator op = root;
        if (op instanceof InnerJoin || op instanceof OuterJoin)
        {
            for (Operator child : op.getChildren())
            {
                checkOperatorOrderJoin(LogicalPlanUtil.getNextClauseOperatorOrSelf(child));
            }
        }
        else
        {
            checkOperatorOrderFrom(op);
        }
    }
    
    /**
     * Checks that the operators in the plan occur in a correct order.
     * 
     * @param root
     *            the root of the plan to check
     */
    private void checkOperatorOrderFrom(Operator root)
    {
        if (root instanceof Ground || root instanceof Scan)
        {
            return;
        }
        if (root instanceof Subquery)
        {
            checkOperatorOrder(((Subquery) root).getChild());
            return;
        }
        
        // Operators are out of order
        throw new AssertionError();
    }
    
    /**
     * Visits and checks the operators in a logical plan.
     * 
     * @param plan
     *            the logical plan to check.
     */
    public void check(LogicalPlan plan)
    {
        for (Assign assign : plan.getAssigns())
        {
            assign.accept(this);
        }
        
        checkOperatorOrder(plan.getRootOperator());
        
        plan.getRootOperator().accept(this);
        
        // Make sure the plan is not nested.
        assert (!plan.isNested());
        
        // Check that the logical plan does not have any free parameters.
        assert (plan.getFreeParametersUsed().isEmpty());
    }
    
    /**
     * Visits and checks the operators in a nested logical plan.
     * 
     * @param plan
     *            the logical plan to check.
     */
    private void checkNested(LogicalPlan plan)
    {
        for (Assign assign : plan.getAssigns())
        {
            assign.accept(this);
        }
        
        plan.getRootOperator().accept(this);
        
        // Make sure the plan is not nested.
        assert (plan.isNested());
    }
    
    /**
     * Performs checks common to all operators in a logical plan and recursively visits the children of the input operator.
     * 
     * @param operator
     *            the operator to check.
     */
    private void checkOperator(Operator operator)
    {
        // Checks that the number of children is correct
        int children_num = operator.getChildren().size();
        if (operator instanceof UnaryOperator)
        {
            assert children_num == 1;
        }
        else if (operator instanceof Ground || operator instanceof Assign)
        {
            assert children_num == 0;
        }
        else if (operator instanceof InnerJoin || operator instanceof OuterJoin)
        {
            assert children_num == 2;
        }
        else
        {
            assert children_num >= 2;
        }
        
        // Recursively visit the children of the input operator.
        List<Operator> children = new ArrayList<Operator>(operator.getChildren());
        
        for (Operator child : children)
        {
            child.accept(this);
        }
        
        // No two children are identical
        for (int i = 0; i < children_num; i++)
            for (int j = i + 1; j < children_num; j++)
                assert (!operator.getChildren().get(i).equals(operator.getChildren().get(j)));
        
        // Check that all variables used by the operator have their binding indexes set, their types set, and either they are
        // relative, that is, they appear in the input, absolute, or they appear in a parameter.
        for (Variable variable : operator.getVariablesUsed())
        {
            assert (variable.getType() != null);
            
            if (variable instanceof AbsoluteVariable)
            {
                assert (variable.getBindingIndex() == -1);
                continue;
            }
            
            assert (variable.getBindingIndex() > -1);
            
            if (!operator.getInputVariables().contains(variable))
            {
                boolean found = false;
                for (Parameter param : operator.getFreeParametersUsed())
                {
                    if (param.getTerm() == variable)
                    {
                        found = true;
                        break;
                    }
                }
                
                assert found;
            }
        }
        
        // Check the consistency of logical plans passed to the operator as arguments
        for (LogicalPlan logical_plan : operator.getLogicalPlansUsed())
        {
            InitialNormalFormChecker new_checker = new InitialNormalFormChecker(m_uas);
            new_checker.checkNested(logical_plan);
        }
    }
    
    /**
     * Performs checks common to all terms in a logical plan given the operator they appear in.
     * 
     * @param term
     *            the term to check.
     * @param operator
     *            the operator the term appears in.
     */
    private void checkTerm(Term term, Operator operator)
    {
        if (term instanceof QueryPath)
        {
            QueryPath query_path = (QueryPath) term;
            assert (!(query_path.getTerm() instanceof QueryPath));
            this.checkTerm(query_path.getTerm(), operator);
        }
        else if (term instanceof Parameter)
        {
            Parameter param = (Parameter) term;
            assert (param.getTerm() instanceof RelativeVariable || param.getTerm() instanceof QueryPath);
            this.checkTerm(param.getTerm(), operator);
        }
        else if (term instanceof FunctionCall)
        {
            FunctionCall<?> func_call = (FunctionCall<?>) term;
            for (Term arg : func_call.getArguments())
                this.checkTerm(arg, operator);
        }
    }
    
    @Override
    public Operator visitApplyPlan(ApplyPlan operator)
    {
        assert (operator.getLogicalPlansUsed().size() == 1);
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitAntiSemiJoin(AntiSemiJoin operator)
    {
        // This operator should not be in the plan
        throw new AssertionError();
    }
    
    @Override
    public Operator visitCopy(Copy operator)
    {
        // This operator should not be in the plan
        throw new AssertionError();
    }
    
    @Override
    public Operator visitEliminateDuplicates(EliminateDuplicates operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitExists(Exists operator)
    {
        // Check that there is an exists plan.
        assert (operator.getLogicalPlansUsed().size() == 1);
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitGround(Ground operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitGroupBy(GroupBy operator)
    {
        // Make sure the type of the group by terms is scalar.
        for (Term term : operator.getGroupByTerms())
        {
            assert (term.getType() instanceof ScalarType);
            this.checkTerm(term, operator);
        }
        
        // Check the aggregate function calls
        for (Aggregate agg : operator.getAggregates())
        {
            this.checkTerm(agg.getAggregateFunctionCall(), operator);
        }
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitInnerJoin(InnerJoin operator)
    {
        // Make sure there are join conditions.
        assert (!operator.getConditions().isEmpty());
        
        // Make sure the type of the join conditions is boolean.
        for (Term condition : operator.getConditions())
        {
            assert (condition.getType() instanceof BooleanType || condition.getType() instanceof NoSqlType);
            this.checkTerm(condition, operator);
        }
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitNavigate(Navigate operator)
    {
        this.checkOperator(operator);
        
        this.checkTerm(operator.getTerm(), operator);
        
        return operator;
    }
    
    @Override
    public Operator visitOffsetFetch(OffsetFetch operator)
    {
        // Make sure that offset or fetch are not null.
        assert (operator.getOffset() != null || operator.getFetch() != null);
        
        // Make sure the type of offset and fetch is integer.
        if (operator.getOffset() != null)
        {
            assert (operator.getOffset().getType() instanceof IntegerType || operator.getOffset().getType() instanceof NoSqlType);
            this.checkTerm(operator.getOffset(), operator);
        }
        if (operator.getFetch() != null)
        {
            assert (operator.getFetch().getType() instanceof IntegerType || operator.getFetch().getType() instanceof NoSqlType);
            this.checkTerm(operator.getFetch(), operator);
        }
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitOuterJoin(OuterJoin operator)
    {
        visitInnerJoin(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitProduct(Product operator)
    {
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitProject(Project operator)
    {
        this.checkOperator(operator);
        
        // Check the projection items
        for (Item item : operator.getProjectionItems())
        {
            this.checkTerm(item.getTerm(), operator);
        }
        
        return operator;
    }
    
    @Override
    public Operator visitScan(Scan operator)
    {
        this.checkOperator(operator);
        
        this.checkTerm(operator.getTerm(), operator);
        
        return operator;
    }
    
    @Override
    public Operator visitSelect(Select operator)
    {
        // Make sure the condition is not null.
        assert (operator.getMergedCondition() != null);
        
        // Make sure the condition's type is boolean.
        assert (operator.getMergedCondition().getType() instanceof BooleanType);
        
        this.checkOperator(operator);
        
        // Check the conditions
        for (Term term : operator.getConditions())
        {
            this.checkTerm(term, operator);
        }
        
        return operator;
    }
    
    @Override
    public Operator visitSemiJoin(SemiJoin operator)
    {
        // This operator should not be in the plan
        throw new AssertionError();
    }
    
    @Override
    public Operator visitSendPlan(SendPlan operator)
    {
        // This operator should not be in the plan
        throw new AssertionError();
    }
    
    @Override
    public Operator visitSetOperator(SetOperator operator)
    {
        if (operator.getSetOpType() == SetOpType.OUTER_UNION)
        {
            // Make sure the two children do not output the same attribute name with incompatible types
            OutputInfo left_info = operator.getChildren().get(0).getOutputInfo();
            OutputInfo right_info = operator.getChildren().get(1).getOutputInfo();
            for (RelativeVariable left_var : left_info.getVariables())
            {
                if (right_info.contains(left_var))
                {
                    RelativeVariable right_var = right_info.getVariable(left_var.getName());
                    TypeUtil.deepEqualsByIsomorphism(left_var.getType(), right_var.getType());
                }
            }
        }
        else
        {
            // Make sure both children have the same output type
            Iterator<RelativeVariable> left_iter = operator.getChildren().get(0).getOutputInfo().getVariables().iterator();
            Iterator<RelativeVariable> right_iter = operator.getChildren().get(1).getOutputInfo().getVariables().iterator();
            while (left_iter.hasNext())
            {
                RelativeVariable left_var = left_iter.next();
                Type left_type = left_var.getType();
                RelativeVariable right_var = right_iter.next();
                Type right_type = right_var.getType();
                assert (TypeUtil.deepEqualsByIsomorphism(left_type, right_type));
            }
            assert (!right_iter.hasNext());
        }
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitSort(Sort operator)
    {
        // Check that there is at least one sort item.
        assert (operator.getSortItems().size() > 0);
        
        this.checkOperator(operator);
        
        // Check the conditions
        for (Sort.Item item : operator.getSortItems())
        {
            this.checkTerm(item.getTerm(), operator);
        }
        
        return operator;
    }
    
    @Override
    public Operator visitCreateSchemaObject(CreateDataObject operator)
    {
        assert (operator.getChildren().size() == 0);
        
        return operator;
    }
    
    @Override
    public Operator visitDropSchemaObject(DropDataObject operator)
    {
        assert (operator.getChildren().size() == 0);
        
        return operator;
    }
    
    @Override
    public Operator visitInsert(Insert operator)
    {
        // Check that there is an insert plan.
        assert (operator.getLogicalPlansUsed().size() == 1);
        
        Term access_term = operator.getTargetTerm();
        this.checkTerm(access_term, operator);
        
        assert (access_term.getType() instanceof CollectionType);
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitDelete(Delete operator)
    {
        this.checkTerm(operator.getTargetTerm(), operator);
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitUpdate(Update operator)
    {
        if (operator.getTargetTerm() != null)
        {
            this.checkTerm(operator.getTargetTerm(), operator);
        }
        for (Assignment assignment : operator.getAssignments())
        {
            this.checkTerm(assignment.getTerm(), operator);
        }
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitPartitionBy(PartitionBy operator)
    {
        // This operator should not be in the plan
        throw new AssertionError();
    }
    
    @Override
    public Operator visitIndexScan(IndexScan operator)
    {
        // This operator should not be in the plan
        throw new AssertionError();
    }
    
    @Override
    public Operator visitAssign(Assign operator)
    {
        // Check that there is an apply plan.
        assert (operator.getLogicalPlansUsed().size() == 1);
        
        this.checkOperator(operator);
        
        return operator;
    }
    
    @Override
    public Operator visitSubquery(Subquery subquery)
    {
        this.checkOperator(subquery);
        
        return subquery;
    }
}
