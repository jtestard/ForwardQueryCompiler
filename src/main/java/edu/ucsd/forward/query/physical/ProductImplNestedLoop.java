package edu.ucsd.forward.query.physical;

import java.util.Collections;

import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.Product;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Represents an implementation of the nested-loop algorithm for the product logical operator. This operator implementation buffers
 * the values of the right child operator implementation on demand.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 */
public class ProductImplNestedLoop extends AbstractBinaryOperatorImplNestedLoop<Product>
{
    /**
     * Determines whether the population of the left side is done.
     */
    private boolean m_population_done;
    
    /**
     * Constructs an instance of the nested-loop join operator implementation.
     * 
     * @param logical
     *            a logical product operator.
     * @param left_child
     *            the left child operator implementation.
     * @param right_child
     *            the right child operator implementation.
     */
    public ProductImplNestedLoop(Product logical, OperatorImpl left_child, OperatorImpl right_child)
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
        
        BindingBuffer buffer = factory.buildBindingBuffer(Collections.<Term> emptyList(), left_info, right_info, false);
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
        
        // There is no left binding
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
                this.setLeftIterator(this.getLeftBindingBuffer().getBindings());
            }
            
            // Get the next left value
            Binding left_value = this.getLeftIterator().next();
            
            // Construct a big binding from the current bindings
            Binding out_binding = new Binding();
            out_binding.addValues(left_value.getValues());
            out_binding.addValues(getRightBinding().getValues());
            
            // Reset the cloned flags since binding values are used in multiple bindings
            out_binding.resetCloned();
            return out_binding;
        }
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        ProductImplNestedLoop copy = new ProductImplNestedLoop(this.getOperator(), this.getLeftChild().copy(context),
                                                               this.getRightChild().copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitProductImplNestedLoop(this);
    }
    
}
