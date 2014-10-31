package edu.ucsd.forward.query.source_wrapper.sql;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.InnerJoin;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.OuterJoin;
import edu.ucsd.forward.query.logical.Product;
import edu.ucsd.forward.query.logical.Select;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.plan.LogicalPlanUtil;
import edu.ucsd.forward.query.logical.plan.SubqueryRewriter;
import edu.ucsd.forward.util.NameGenerator;

/**
 * The module that takes a plan in the distributed normal form and rewrites it into the SQL normal form for easier translation. For
 * now, it only inserts subqueries above pushed-down selections.
 * 
 * @author Romain Vernoux
 */
public class SqlNormalFormRewriter
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(SqlNormalFormRewriter.class);
    
    /**
     * Default constructor.
     */
    public SqlNormalFormRewriter()
    {
        
    }
    
    /**
     * Takes a plan in the distributed normal form and rewrites it into the initial normal form. For now, it only inserts subqueries
     * above pushed-down selections.
     * 
     * @param input
     *            the plan to rewrite
     * @throws QueryCompilationException
     *             if an error occurs during the process
     */
    public void rewrite(LogicalPlan input) throws QueryCompilationException
    {
        Operator root = input.getRootOperator();
        for (Operator op : root.getDescendantsAndSelf())
        {
            for (LogicalPlan used_plans : op.getLogicalPlansUsed())
            {
                rewrite(used_plans);
            }
            
            if (op instanceof Select)
            {
                Operator prev_clause_op = LogicalPlanUtil.getPreviousClauseOperator(op);
                if (prev_clause_op instanceof InnerJoin || prev_clause_op instanceof OuterJoin
                        || prev_clause_op instanceof Product)
                {
                    SubqueryRewriter rewriter = new SubqueryRewriter(NameGenerator.SQL_SOURCE_WRAPPER_GENERATOR);
                    rewriter.insertSubquery(op);
                }
            }
        }
    }
}
