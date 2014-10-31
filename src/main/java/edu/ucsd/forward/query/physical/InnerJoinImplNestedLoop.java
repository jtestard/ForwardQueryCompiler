package edu.ucsd.forward.query.physical;

import java.util.Collection;

import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.InnerJoin;
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * The implementation of the nested-loop algorithm for the join logical operator. This operator implementation buffers the values of
 * the left child operator implementation.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 */
public class InnerJoinImplNestedLoop extends AbstractBinaryOperatorImplNestedLoop<InnerJoin>
{
    /**
     * Determines whether the population of the left side is done.
     */
    private boolean m_population_done;
    
    /**
     * Constructs an instance of the nested-loop join operator implementation.
     * 
     * @param logical
     *            a logical join operator.
     * @param left_child
     *            the left child operator implementation.
     * @param right_child
     *            the right child operator implementation.
     */
    public InnerJoinImplNestedLoop(InnerJoin logical, OperatorImpl left_child, OperatorImpl right_child)
    {
        super(logical, left_child, right_child);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        // Construct the binding buffer.
        BindingBufferFactory factory = BindingBufferFactory.getInstance();
        m_population_done = false;
        
        OutputInfo left_info = this.getLeftChild().getOperator().getOutputInfo();
        OutputInfo right_info = this.getRightChild().getOperator().getOutputInfo();
        Collection<Term> join_conditions = this.getOperator().getConditions();
        
        BindingBuffer buffer = factory.buildBindingBuffer(join_conditions, left_info, right_info, false);
        this.setLeftBindingBuffer(buffer);
        
        super.open();
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        // Populate the left buffer
        if (!m_population_done)
        {
            this.getLeftBindingBuffer().populate(this.getLeftChild());
            m_population_done = true;
        }
        
        // Fail fast if there is no left binding
        if (this.getLeftBindingBuffer().isEmpty())
        {
            return null;
        }
        
        while (true)
        {
            // Get the next right value
            if (this.getRightBinding() == null || !this.getLeftIterator().hasNext())
            {
                this.setRightBinding(this.getRightChild().next());
                
                // The right side is exhausted
                if (getRightBinding() == null) return null;
                
                // Reset and the left iterator
                this.setLeftIterator(this.getLeftBindingBuffer().get(this.getRightBinding(), true));
            }
            
            // There are no matching bindings
            if (!this.getLeftIterator().hasNext()) continue;
            
            // Get the next matching binding
            Binding out_binding = this.getLeftIterator().next();
            
            // Reset the cloned flags since binding values are used in multiple bindings
            out_binding.resetCloned();
            
            return out_binding;
        }
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        InnerJoinImplNestedLoop copy = new InnerJoinImplNestedLoop(this.getOperator(), this.getLeftChild().copy(context),
                                                                   this.getRightChild().copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitInnerJoinImplNestedLoop(this);
    }
    
}
