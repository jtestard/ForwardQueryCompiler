package edu.ucsd.forward.query.physical;

import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.EagerQueryResult;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.ApplyPlan;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Represents an implementation of the apply plan interface.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 */
public class ApplyPlanImpl extends AbstractUnaryOperatorImpl<ApplyPlan>
{
    private static final long serialVersionUID = 1L;
    
    /**
     * The physical plan to apply.
     */
    private PhysicalPlan      m_apply_plan;
    
    /**
     * The current binding.
     */
    private Binding           m_in_binding;
    
    /**
     * The result holds the nested plan result.
     */
    private EagerQueryResult  m_query_result;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical apply plan operator.
     * @param child
     *            the single child operator implementation.
     */
    public ApplyPlanImpl(ApplyPlan logical, OperatorImpl child)
    {
        super(logical);
        
        assert (child != null);
        this.addChild(child);
    }
    
    /**
     * Sets the physical plan to apply.
     * 
     * @param apply
     *            the physical plan to apply.
     */
    public void setApplyPlan(PhysicalPlan apply)
    {
        assert (apply != null);
        m_apply_plan = apply;
    }
    
    @Override
    public List<PhysicalPlan> getPhysicalPlansUsed()
    {
        return Collections.<PhysicalPlan> singletonList(m_apply_plan);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        super.open();
        m_in_binding = null;
        m_query_result = null;
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        boolean short_circuited = false;
        
        // Get the next value from the child operator implementation
        if (m_query_result == null)
        {
            m_in_binding = this.getChild().next();
            if (m_in_binding == null)
            {
                return null;
            }
            
            // Instantiate parameters
            this.instantiateBoundParameters(m_in_binding);
            
            
            // Evaluate the execution condition
            if(this.getOperator().hasExecutionCondition())
            {
                Value exec_cond = TermEvaluator.evaluate(this.getOperator().getExecutionCondition(), m_in_binding).getValue();
                if (exec_cond instanceof BooleanValue)
                {
                    short_circuited = !(((BooleanValue) exec_cond).getObject());
                }
                else
                {
                    assert (exec_cond instanceof NullValue);
                    short_circuited = true;
                }
            }
            
            if(!short_circuited)
            {
                // Fully execute the apply physical plan
                QueryProcessor qp = QueryProcessorFactory.getInstance();
                m_query_result = qp.createEagerQueryResult(m_apply_plan, qp.getUnifiedApplicationState());
            }
        }
        
        Value nested_value;
        if(short_circuited)
        {
            nested_value = new NullValue(CollectionType.class);
            
            // Augment the input binding with the nested collection value
            m_in_binding.addValue(new BindingValue(nested_value, true));
        }
        else
        {
            nested_value = m_query_result.getValueWithSuspension();

            // Augment the input binding with the nested collection value
            m_in_binding.addValue(new BindingValue(nested_value, false));
        }
        
        m_query_result = null;
        
        
        return m_in_binding;
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        ApplyPlanImpl copy = new ApplyPlanImpl(this.getOperator(), this.getChild().copy(context));
        
        copy.setApplyPlan(m_apply_plan.copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitApplyPlanImpl(this);
    }
    
}
