package edu.ucsd.forward.query.physical;

import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.Exists;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Represents an implementation of the exists interface.
 * 
 * @author Michalis Petropoulos
 */
@SuppressWarnings("serial")
public class ExistsImpl extends AbstractUnaryOperatorImpl<Exists>
{
    /**
     * The physical plan to check.
     */
    private PhysicalPlan m_exists_plan;
    
    /**
     * The current binding.
     */
    private Binding      m_in_binding;
    
    /**
     * the exists pipeline.
     */
    private Pipeline     m_nested_pipeline;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical exists operator.
     * @param child
     *            the single child operator implementation.
     */
    public ExistsImpl(Exists logical, OperatorImpl child)
    {
        super(logical);
        
        assert (child != null);
        this.addChild(child);
    }
    
    /**
     * Sets the physical plan to check.
     * 
     * @param exists
     *            the physical plan to check.
     */
    public void setExistsPlan(PhysicalPlan exists)
    {
        assert (exists != null);
        m_exists_plan = exists;
    }
    
    @Override
    public List<PhysicalPlan> getPhysicalPlansUsed()
    {
        return Collections.<PhysicalPlan> singletonList(m_exists_plan);
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        boolean short_circuited = false;
        
        if (m_nested_pipeline == null)
        {
            // Get the next value from the child operator implementation
            m_in_binding = this.getChild().next();
            
            if (m_in_binding == null) return null;
            
            // Instantiate parameters
            this.instantiateBoundParameters(m_in_binding);
            
            // Evaluate the execution condition
            if (this.getOperator().hasExecutionCondition())
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
            
            if (!short_circuited)
            {
                // Get only the first binding of the exists pipeline
                QueryProcessor qp = QueryProcessorFactory.getInstance();
                m_nested_pipeline = qp.createLazyQueryResult(m_exists_plan, qp.getUnifiedApplicationState());
                
                // Open the pipeline
                m_nested_pipeline.open();
            }
        }
        
        Value nested_value;
        if (short_circuited)
        {
            nested_value = new NullValue(BooleanType.class);
        }
        else
        {
            Binding next_binding = m_nested_pipeline.next();
            
            m_nested_pipeline.close();
            m_nested_pipeline = null;
            
            // Augment the input binding with the nested collection value
            if (next_binding == null)
            {
                nested_value = new BooleanValue(false);
            }
            else
            {
                nested_value = new BooleanValue(true);
            }
        }
        
        m_in_binding.addValue(new BindingValue(nested_value, true));
        
        return m_in_binding;
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        ExistsImpl copy = new ExistsImpl(this.getOperator(), this.getChild().copy(context));
        
        copy.setExistsPlan(m_exists_plan.copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitExistsImpl(this);
    }
    
}
