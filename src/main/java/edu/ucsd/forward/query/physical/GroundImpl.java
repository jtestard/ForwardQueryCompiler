package edu.ucsd.forward.query.physical;

import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.Ground;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;

/**
 * Implementation of the ground logical operator.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class GroundImpl extends AbstractOperatorImpl<Ground>
{
    /**
     * Indicates whether the single binding has been returned.
     */
    private boolean m_done;
    
    /**
     * Constructs an instance of a data object ground operator implementation.
     * 
     * @param logical
     *            a logical data object ground operator.
     */
    public GroundImpl(Ground logical)
    {
        super(logical);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        super.open();
        
        m_done = false;
    }
    
    @Override
    public Binding next() throws QueryExecutionException
    {
        if (m_done) return null;
        
        // Create a binding
        Binding binding = new Binding();
        m_done = true;
        
        return binding;
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        GroundImpl copy = new GroundImpl(this.getOperator());
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitGroundImpl(this);
    }
    
}
