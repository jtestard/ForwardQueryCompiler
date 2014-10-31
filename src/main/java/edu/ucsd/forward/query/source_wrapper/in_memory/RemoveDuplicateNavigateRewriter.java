package edu.ucsd.forward.query.source_wrapper.in_memory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.function.aggregate.AggregateFunctionCall;
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
import edu.ucsd.forward.query.logical.GroupBy.Aggregate;
import edu.ucsd.forward.query.logical.GroupBy.Item;
import edu.ucsd.forward.query.logical.ddl.CreateDataObject;
import edu.ucsd.forward.query.logical.ddl.DropDataObject;
import edu.ucsd.forward.query.logical.dml.Delete;
import edu.ucsd.forward.query.logical.dml.Insert;
import edu.ucsd.forward.query.logical.dml.Update;
import edu.ucsd.forward.query.logical.dml.Update.Assignment;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.plan.LogicalPlanUtil;
import edu.ucsd.forward.query.logical.term.ElementVariable;
import edu.ucsd.forward.query.logical.term.PositionVariable;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.TermUtil;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.AbstractOperatorVisitor;

/**
 * The module that remove duplicate Navigate operators. A Navigate operator is called "duplicate" if the result of its navigation is
 * used by an operator and this operators has in its input binding another variable that corresponds to the same navigation.
 * 
 * @author Romain Vernoux
 */
public class RemoveDuplicateNavigateRewriter
{
    @SuppressWarnings("unused")
    private static final Logger             log = Logger.getLogger(RemoveDuplicateNavigateRewriter.class);
    
    /**
     * The map of substitutions for relative variables used by this rewriter.
     */
    private Map<Term, Term>                 m_substitutions;
    
    /**
     * The map that stores all the navigate operators in the plan for fast access
     */
    private Map<RelativeVariable, Navigate> m_navigate_vars;
    
    /**
     * Default constructor.
     */
    public RemoveDuplicateNavigateRewriter()
    {
        m_substitutions = new HashMap<Term, Term>();
        m_navigate_vars = new HashMap<RelativeVariable, Navigate>();
    }
    
    /**
     * Takes a plan in the distributed normal and remove duplicate Navigate operators. A Navigate operator is called "duplicate" if
     * the result of its navigation is used by an operator and this operators has in its input binding another variable that
     * corresponds to the same navigation.
     * 
     * @param input
     *            the plan to rewrite
     * @throws QueryCompilationException
     *             if an error occurs during the process
     */
    public void rewrite(LogicalPlan input) throws QueryCompilationException
    {
        // Stack the logical operators for a bottom-up and right-to-left traversal
        Stack<Operator> stack_logical = new Stack<Operator>();
        LogicalPlanUtil.stackBottomUpRightToLeft(input.getRootOperator(), stack_logical);
        
        // Remove the duplicate Navigate bottom-up
        while (!stack_logical.isEmpty())
        {
            Operator current_op = stack_logical.pop();
            if (current_op instanceof Navigate)
            {
                Navigate current_nav = (Navigate) current_op;
                RelativeVariable duplicate = getDuplicate(current_nav);
                if (duplicate != null)
                {
                    // Remove the current Navigate operator and update the variable references above
                    Operator child = current_nav.getChild();
                    LogicalPlanUtil.remove(current_op);
                    m_substitutions.clear();
                    m_substitutions.put(current_nav.getAliasVariable(), duplicate);
                    VariableReferencesUpdater var_ref_updater = new VariableReferencesUpdater();
                    Operator op = child;
                    do
                    {
                        op = op.getParent();
                        if (op == null) break;
                        op.accept(var_ref_updater);
                        op.updateOutputInfo();
                    } while (!(op instanceof Project || op instanceof GroupBy));
                }
                else
                {
                    m_navigate_vars.put(current_nav.getAliasVariable(), current_nav);
                }
            }
        }
    }
    
    /**
     * Checks if a Navigate is a duplicate, and if so, returns a relative variable of the deepest Navigate operator that provides
     * the same navigation.
     * 
     * @param nav
     *            the Navigate operator
     * @return the relative variable of a Navigate that provides the same navigation, or <code>null</code> if none is found.
     */
    private RelativeVariable getDuplicate(Navigate nav)
    {
        for (Variable in_var : nav.getChild().getOutputInfo().getVariables())
        {
            if (in_var instanceof ElementVariable || in_var instanceof PositionVariable)
            {
                continue;
            }
            if (!(in_var instanceof RelativeVariable))
            {
                continue;
            }
            RelativeVariable rel_var = (RelativeVariable) in_var;
            
            Navigate duplicate_nav = m_navigate_vars.get(rel_var);
            if(duplicate_nav == null)
            {
                continue;
            }

            Term duplicate_term = duplicate_nav.getTerm();
            Term original_term = nav.getTerm();
            if (!duplicate_term.getClass().equals(original_term.getClass()))
            {
                continue;
            }
            
            // General case
            if (duplicate_term.equals(original_term))
            {
                return rel_var;
            }
            
            // Handle query paths
            if (original_term instanceof QueryPath)
            {
                QueryPath original_qp = (QueryPath) original_term;
                QueryPath duplicate_qp = (QueryPath) duplicate_term;
                if (original_qp.getTerm().equals(duplicate_qp.getTerm())
                        && original_qp.getPathSteps().equals(duplicate_qp.getPathSteps()))
                {
                    return rel_var;
                }
            }
        }
        
        // No duplicate found
        return null;
    }
    
    /**
     * The rewriter that updates the variable references inside the operators.
     * 
     * @author Romain Vernoux
     */
    private class VariableReferencesUpdater extends AbstractOperatorVisitor
    {
        @Override
        public Operator visitAntiSemiJoin(AntiSemiJoin operator)
        {
            return visitInnerJoin(operator);
        }
        
        @Override
        public Operator visitAssign(Assign operator)
        {
            // Cannot be below a Project
            throw new AssertionError();
        }
        
        @Override
        public Operator visitApplyPlan(ApplyPlan operator)
        {
            if (operator.hasExecutionCondition())
            {
                operator.setExecutionCondition(TermUtil.substitute(operator.getExecutionCondition(), m_substitutions));
            }
            return operator;
        }
        
        @Override
        public Operator visitCopy(Copy operator)
        {
            // Should not be encountered
            throw new AssertionError();
        }
        
        @Override
        public Operator visitEliminateDuplicates(EliminateDuplicates operator)
        {
            return operator;
        }
        
        @Override
        public Operator visitExists(Exists operator)
        {
            if (operator.hasExecutionCondition())
            {
                operator.setExecutionCondition(TermUtil.substitute(operator.getExecutionCondition(), m_substitutions));
            }
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
            for (Item item : operator.getGroupByItems())
            {
                item.setTerm(TermUtil.substitute(item.getTerm(), m_substitutions));
            }
            for (Item item : operator.getCarryOnItems())
            {
                item.setTerm(TermUtil.substitute(item.getTerm(), m_substitutions));
            }
            for (Aggregate agg : operator.getAggregates())
            {
                agg.setAggregateFunctionCall((AggregateFunctionCall) TermUtil.substitute(agg.getAggregateFunctionCall(),
                                                                                         m_substitutions));
            }
            return operator;
        }
        
        @Override
        public Operator visitInnerJoin(InnerJoin operator)
        {
            List<Term> conditions = new ArrayList<Term>(operator.getConditions());
            for (int i = 0; i < conditions.size(); i++)
            {
                Term cond = conditions.get(i);
                operator.removeCondition(cond);
            }
            for (int i = 0; i < conditions.size(); i++)
            {
                Term cond = conditions.get(i);
                operator.addCondition(TermUtil.substitute(cond, m_substitutions));
            }
            return operator;
        }
        
        @Override
        public Operator visitNavigate(Navigate operator)
        {
            operator.setTerm(TermUtil.substitute(operator.getTerm(), m_substitutions));
            return operator;
        }
        
        @Override
        public Operator visitOffsetFetch(OffsetFetch operator)
        {
            return operator;
        }
        
        @Override
        public Operator visitOuterJoin(OuterJoin operator)
        {
            List<Term> conditions = new ArrayList<Term>(operator.getConditions());
            for (int i = 0; i < conditions.size(); i++)
            {
                Term cond = conditions.get(i);
                operator.removeCondition(cond);
            }
            for (int i = 0; i < conditions.size(); i++)
            {
                Term cond = conditions.get(i);
                operator.addCondition(TermUtil.substitute(cond, m_substitutions));
            }
            return operator;
        }
        
        @Override
        public Operator visitPartitionBy(PartitionBy operator)
        {
            List<RelativeVariable> partitionby_terms = new ArrayList<RelativeVariable>(operator.getPartitionByTerms());
            for (int i = 0; i < partitionby_terms.size(); i++)
            {
                RelativeVariable rel_var = partitionby_terms.get(i);
                operator.removePartitionByTerm(rel_var);
            }
            for (int i = 0; i < partitionby_terms.size(); i++)
            {
                RelativeVariable rel_var = partitionby_terms.get(i);
                operator.addPartitionByTerm((RelativeVariable) TermUtil.substitute(rel_var, m_substitutions));
            }
            for (Sort.Item item : operator.getSortByItems())
            {
                item.setTerm(TermUtil.substitute(item.getTerm(), m_substitutions));
            }
            return operator;
        }
        
        @Override
        public Operator visitProduct(Product operator)
        {
            return operator;
        }
        
        @Override
        public Operator visitProject(Project operator)
        {
            for (Project.Item item : operator.getProjectionItems())
            {
                item.setTerm(TermUtil.substitute(item.getTerm(), m_substitutions));
            }
            return operator;
        }
        
        @Override
        public Operator visitScan(Scan operator)
        {
            operator.setTerm(TermUtil.substitute(operator.getTerm(), m_substitutions));
            return operator;
        }
        
        @Override
        public Operator visitSelect(Select operator)
        {
            List<Term> conditions = new ArrayList<Term>(operator.getConditions());
            for (int i = 0; i < conditions.size(); i++)
            {
                Term cond = conditions.get(i);
                operator.removeCondition(cond);
            }
            for (int i = 0; i < conditions.size(); i++)
            {
                Term cond = conditions.get(i);
                operator.addCondition(TermUtil.substitute(cond, m_substitutions));
            }
            return operator;
        }
        
        @Override
        public Operator visitSemiJoin(SemiJoin operator)
        {
            return visitInnerJoin(operator);
        }
        
        @Override
        public Operator visitSendPlan(SendPlan operator)
        {
            // Should not be in the plan
            throw new AssertionError();
        }
        
        @Override
        public Operator visitSetOperator(SetOperator operator)
        {
            return operator;
        }
        
        @Override
        public Operator visitSort(Sort operator)
        {
            for (edu.ucsd.forward.query.logical.Sort.Item item : operator.getSortItems())
            {
                item.setTerm(TermUtil.substitute(item.getTerm(), m_substitutions));
            }
            return operator;
        }
        
        @Override
        public Operator visitCreateSchemaObject(CreateDataObject operator)
        {
            return operator;
        }
        
        @Override
        public Operator visitDropSchemaObject(DropDataObject operator)
        {
            return operator;
        }
        
        @Override
        public Operator visitInsert(Insert operator)
        {
            return operator;
        }
        
        @Override
        public Operator visitDelete(Delete operator)
        {
            return operator;
        }
        
        @Override
        public Operator visitUpdate(Update operator)
        {
            for (Assignment assign : operator.getAssignments())
            {
                assign.setTerm(TermUtil.substitute(assign.getTerm(), m_substitutions));
            }
            return operator;
        }
        
        @Override
        public Operator visitIndexScan(IndexScan operator)
        {
            // Should not be in the plan
            throw new AssertionError();
        }
        
        @Override
        public Operator visitSubquery(Subquery operator)
        {
            return operator;
        }
        
    }
}
