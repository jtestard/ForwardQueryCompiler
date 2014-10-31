/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.EliminateDuplicates;
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Represents an implementation of hash-based duplicate eliminating algorithm for eliminate duplicate logical operator.
 * 
 * @author Yupeng
 * 
 */
@SuppressWarnings("serial")
public class EliminateDuplicatesImpl extends AbstractUnaryOperatorImpl<EliminateDuplicates>
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(EliminateDuplicatesImpl.class);
    
    /**
     * The buffer for the child operator implementation distinct bindings.
     */
    private BindingBuffer       m_index;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical selection operator.
     * @param child
     *            the single child operator implementation.
     */
    public EliminateDuplicatesImpl(EliminateDuplicates logical, OperatorImpl child)
    {
        super(logical);
        
        assert (child != null);
        
        this.addChild(child);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        OutputInfo in_info = this.getOperator().getChild().getOutputInfo();
        
        // Conditions to be used by the binding buffer
        List<Term> conditions = new ArrayList<Term>(getOperator().getConditions());
        
        m_index = new BindingBufferHash(conditions, in_info, false);
        
        super.open();
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        Binding out_binding = this.getChild().next();
        while (out_binding != null)
        {
            // If there is no matching binding, then output the current one
            if (!m_index.get(out_binding, false).hasNext())
            {
                m_index.add(out_binding);
                
                return out_binding;
            }
            
            // Else fetch the next binding
            out_binding = this.getChild().next();
        }
        
        return null;
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        if (this.getState() == State.OPEN)
        {
            m_index.clear();
        }
        
        super.close();
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        EliminateDuplicatesImpl copy = new EliminateDuplicatesImpl(this.getOperator(), this.getChild().copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitEliminateDuplicatesImpl(this);
    }
}
