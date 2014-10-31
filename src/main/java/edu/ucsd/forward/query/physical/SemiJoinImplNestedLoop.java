/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.Collection;
import java.util.Iterator;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.SemiJoin;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Represents an implementation of the nested-loop algorithm for the semi-join logical operator. This operator implementation
 * buffers the values of the right child operator implementation on demand.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public class SemiJoinImplNestedLoop extends AbstractBinaryOperatorImplNestedLoop<SemiJoin>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(SemiJoinImplNestedLoop.class);
    
    /**
     * Determines whether the population of the left side is done.
     */
    private boolean             m_population_done;
    
    /**
     * Constructs an instance of the nested-loop semi-join operator implementation.
     * 
     * @param logical
     *            a logical join operator.
     * @param left_child
     *            the left child operator implementation.
     * @param right_child
     *            the right child operator implementation.
     */
    public SemiJoinImplNestedLoop(SemiJoin logical, OperatorImpl left_child, OperatorImpl right_child)
    {
        super(logical, left_child, right_child);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        m_population_done = false;
        
        // Construct the binding buffer.
        BindingBufferFactory factory = BindingBufferFactory.getInstance();
        
        OutputInfo left_info = this.getLeftChild().getOperator().getOutputInfo();
        OutputInfo right_info = this.getRightChild().getOperator().getOutputInfo();
        Collection<Term> join_conditions = this.getOperator().getConditions();
        
        BindingBuffer buffer = factory.buildBindingBuffer(join_conditions, left_info, right_info, true);
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
                // Remove form the left buffer the left bindings that matched with the previous right binding
                Iterator<Binding> matched_bindings = this.getLeftBindingBuffer().getMatchedBindings();
                while (matched_bindings.hasNext())
                {
                    this.getLeftBindingBuffer().remove(matched_bindings.next());
                }
                this.getLeftBindingBuffer().clearMatchedBindings();
                
                // Get the next right value
                this.setRightBinding(this.getRightChild().next());
                
                // The right side is exhausted
                if (getRightBinding() == null) return null;
                
                // Reset the left iterator
                this.setLeftIterator(this.getLeftBindingBuffer().get(this.getRightBinding(), false));
            }
            
            // There are no matching bindings
            if (!this.getLeftIterator().hasNext()) continue;
            
            // There are no matching bindings
            Binding out_binding = this.getLeftIterator().next();
            
            // Output the left binding
            return out_binding;
        }
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        SemiJoinImplNestedLoop copy = new SemiJoinImplNestedLoop(this.getOperator(), this.getLeftChild().copy(context),
                                                                 this.getRightChild().copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitSemiJoinImplNestedLoop(this);
    }
    
}
