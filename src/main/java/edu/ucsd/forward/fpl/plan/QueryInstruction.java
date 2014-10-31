/**
 * 
 */
package edu.ucsd.forward.fpl.plan;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.fpl.FplInterpretationException;
import edu.ucsd.forward.query.EagerQueryResult;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * A query instruction that executes a query.
 * 
 * @author Yupeng
 * 
 */
public class QueryInstruction extends AbstractInstruction
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(QueryInstruction.class);
    
    private PhysicalPlan        m_query_plan;
    
    /**
     * Constructs the query instruction with a query plan.
     * 
     * @param query_plan
     *            the query plan
     */
    public QueryInstruction(PhysicalPlan query_plan)
    {
        setQueryPlan(query_plan);
    }
    
    /**
     * Sets the query plan to execute.
     * 
     * @param query_plan
     *            the query plan to execute.
     */
    public void setQueryPlan(PhysicalPlan query_plan)
    {
        assert query_plan != null;
        m_query_plan = query_plan;
    }
    
    /**
     * Gets the query plan.
     * 
     * @return the query plan.
     */
    public PhysicalPlan getQueryPlan()
    {
        return m_query_plan;
    }
    
    @Override
    public String toExplainString()
    {
        return super.toExplainString() + m_query_plan.toExplainString();
    }
    
    @Override
    public String execute(UnifiedApplicationState uas) throws FplInterpretationException
    {
        // Executes the query plan
        EagerQueryResult result = QueryProcessorFactory.getInstance().createEagerQueryResult(m_query_plan, uas);
        result.getValue();
        return null;
    }
}
