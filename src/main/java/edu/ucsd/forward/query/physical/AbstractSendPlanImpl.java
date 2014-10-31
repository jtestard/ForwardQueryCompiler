package edu.ucsd.forward.query.physical;

import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.SendPlan;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.Parameter.InstantiationMethod;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Represents an abstract implementation of the send plan interface that sends a physical plan to a target data source for
 * execution.
 * 
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractSendPlanImpl extends AbstractOperatorImpl<SendPlan> implements SendPlanImpl
{
    /**
     * The physical plan to send.
     */
    private PhysicalPlan m_send_plan;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical send plan operator.
     */
    public AbstractSendPlanImpl(SendPlan logical)
    {
        super(logical);
    }
    
    /**
     * Sets the physical plan to send.
     * 
     * @param send
     *            the physical plan to send.
     */
    public void setSendPlan(PhysicalPlan send)
    {
        assert (send != null);
        m_send_plan = send;
    }
    
    @Override
    public PhysicalPlan getSendPlan()
    {
        return m_send_plan;
    }
    
    @Override
    public List<PhysicalPlan> getPhysicalPlansUsed()
    {
        return Collections.<PhysicalPlan> singletonList(m_send_plan);
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        // FIXME The SendPlan operator is supposed to have many Copy operators and at most one non-copy operator
        OperatorImpl non_copy_impl = null;
        // Execute the copy operator implementation children
        for (OperatorImpl child : this.getChildren())
        {
            if (child instanceof CopyImpl)
            {
                assert (child.next() == null);
            }
            else
            {
                non_copy_impl = child;
            }
        }
        
        Binding in_binding = (non_copy_impl == null) ? null : non_copy_impl.next();
        
        // Instantiate parameters
        if (in_binding != null)
        {
            QueryProcessor qp = QueryProcessorFactory.getInstance();
            ParameterInstantiations param_insts = qp.getParameterInstantiations();
            for (Parameter param : this.getOperator().getBoundParametersUsed())
            {
                // Skip the COPY parameters
                if (param.getInstantiationMethod() == InstantiationMethod.COPY) continue;
                
                BindingValue value = TermEvaluator.evaluate(param.getTerm(), in_binding);
                value.resetCloned();
                param_insts.setInstantiation(param, value);
            }
        }
        
        return in_binding;
    }
    
}
