/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.Collection;
import java.util.Iterator;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.AntiSemiJoin;
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * The implementation of the nested-loop algorithm for the anti-join logical operator.
 * 
 * @author Yupeng
 * 
 */
public class AntiSemiJoinImplNestedLoop extends AbstractBinaryOperatorImplNestedLoop<AntiSemiJoin>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AntiSemiJoinImplNestedLoop.class);
    
    /**
     * Determines whether the right side is exhausted.
     */
    private boolean             m_right_exhausted;
    
    /**
     * Determines whether the population of the left side is done.
     */
    private boolean             m_population_done;
    
    /**
     * Constructs an instance of the nested-loop anti-join operator implementation.
     * 
     * @param logical
     *            a logical join operator.
     * @param left_child
     *            the left child operator implementation.
     * @param right_child
     *            the right child operator implementation.
     */
    public AntiSemiJoinImplNestedLoop(AntiSemiJoin logical, OperatorImpl left_child, OperatorImpl right_child)
    {
        super(logical, left_child, right_child);
    }
    
    /**
     * Construct the binding buffer.
     */
    private void init()
    {
        m_right_exhausted = false;
        m_population_done = false;
        
        BindingBufferFactory factory = BindingBufferFactory.getInstance();
        
        OutputInfo left_info = this.getLeftChild().getOperator().getOutputInfo();
        OutputInfo right_info = this.getRightChild().getOperator().getOutputInfo();
        Collection<Term> join_conditions = this.getOperator().getConditions();
        
        BindingBuffer buffer = factory.buildBindingBuffer(join_conditions, left_info, right_info, true);
        this.setLeftBindingBuffer(buffer);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        init();
        
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
        if (this.getLeftBindingBuffer().isEmpty()) return null;
        
        // Exhaust the right side, remove the matching bindings from the buffer, and output the remaining left bindings
        while (!m_right_exhausted)
        {
            this.setRightBinding(this.getRightChild().next());
            
            // Remove form the left buffer the left bindings that matched with the previous right binding
            Iterator<Binding> matched_bindings = this.getLeftBindingBuffer().getMatchedBindings();
            while (matched_bindings.hasNext())
            {
                this.getLeftBindingBuffer().remove(matched_bindings.next());
            }
            this.getLeftBindingBuffer().clearMatchedBindings();
            
            // The right side is exhausted
            if (getRightBinding() == null)
            {
                m_right_exhausted = true;
                // Set the left iterator to the remaining left bindings
                this.setLeftIterator(this.getLeftBindingBuffer().getBindings());
                break;
            }
            
            // Reset the left iterator
            this.setLeftIterator(this.getLeftBindingBuffer().get(this.getRightBinding(), false));
        }
        
        // Output the remaining bindings in the left buffer
        while (this.getLeftIterator().hasNext())
        {
            return this.getLeftIterator().next();
        }
        
        return null;
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        AntiSemiJoinImplNestedLoop copy = new AntiSemiJoinImplNestedLoop(this.getOperator(), this.getLeftChild().copy(context),
                                                                         this.getRightChild().copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitAntiSemiJoinImplNestedLoop(this);
    }
    
}
