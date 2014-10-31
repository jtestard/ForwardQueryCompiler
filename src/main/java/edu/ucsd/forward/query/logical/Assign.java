/**
 * 
 */
package edu.ucsd.forward.query.logical;

import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

/**
 * The Assign operator has a dual role: On the one hand, it provides a way to represent common sub-expressions in a plan. On the
 * other hand, it provides the means to represent sub-plans whose execution source differs from that of their enclosing plan. It
 * evaluates plan p in the context of each tuple r. The result of the evaluation is stored in the temporal variable i available for
 * the life-time of the enclosing plan R. In this way, assigning the sub-plan to a temporal target variable can be thought of as
 * defining temporary data objects that exist just for the enclosing plan.
 * 
 * @author Yupeng Fu
 * 
 */
public class Assign extends AbstractOperator
{
    private static final long serialVersionUID = 1L;
    
    /**
     * The assignment plan.
     */
    private LogicalPlan       m_plan;
    
    /**
     * The assigned target.
     */
    private String            m_target;
    
    /**
     * Constructs an Assign operator.
     * 
     * @param plan
     *            the assigned plan
     * @param target
     *            the target
     */
    public Assign(LogicalPlan plan, String target)
    {
        super();
        assert plan != null;
        m_plan = plan;
        
        assert target != null;
        m_target = target;
    }
    
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(Assign.class);
    
    /**
     * Gets the assigned plan.
     * 
     * @return the assigned plan.
     */
    public LogicalPlan getPlan()
    {
        return m_plan;
    }
    
    /**
     * Gets the assigned target.
     * 
     * @return the assigned target.
     */
    public String getTarget()
    {
        return m_target;
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitAssign(this);
    }
    
    @Override
    public Operator copy()
    {
        Assign copy = new Assign(m_plan, m_target);
        super.copy(copy);
        return copy;
    }
    
    @Override
    public void updateOutputInfo() throws QueryCompilationException
    {
        // No need to update the output info of the nested logical plan
        
        // There is no output information.
        this.setOutputInfo(new OutputInfo());
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        return Collections.emptyList();
    }
    
    @Override
    public List<Parameter> getBoundParametersUsed()
    {
        return Collections.emptyList();
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        return m_plan.getFreeParametersUsed();
    }
    
    @Override
    public List<LogicalPlan> getLogicalPlansUsed()
    {
        return Collections.<LogicalPlan> singletonList(m_plan);
    }
    
    @Override
    public String toExplainString()
    {
        String str = super.toExplainString() + " -> " + m_target;
        
        return str;
    }
}
