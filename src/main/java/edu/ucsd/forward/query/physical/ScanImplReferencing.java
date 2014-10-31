/**
 * 
 */
package edu.ucsd.forward.query.physical;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.term.AbsoluteVariable;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Represents the scan implementation that references the data object created by an assign operator in the plan.
 * 
 * @author Yupeng Fu
 * 
 */
public class ScanImplReferencing extends AbstractUnaryOperatorImpl<Scan>
{
    private static final long   serialVersionUID = 1L;
    
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(ScanImplReferencing.class);
    
    /**
     * The assign implementation that this scan is referencing.
     */
    private AbstractAssignImpl          m_assign_impl;
    
    /**
     * Constructor.
     * 
     * @param logical
     *            the logical operator.
     * @param child
     *            the child operator.
     */
    public ScanImplReferencing(Scan logical, OperatorImpl child)
    {
        super(logical, child);
    }
    
    /**
     * Sets the assign implementation that the scan references.
     * 
     * @param assign_impl
     *            the referenced assign impl.
     */
    public void setReferencedAssign(AbstractAssignImpl assign_impl)
    {
        m_assign_impl = assign_impl;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitScanImplReferencing(this);
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        ScanImplReferencing copy = new ScanImplReferencing(getOperator(), getChild().copy(context));
        // Copy the referenced assign impl.
        AbsoluteVariable term = (AbsoluteVariable) getOperator().getTerm();
        String target = term.getSchemaObjectName();
        if (context.containsAssignTarget(target))
        {
            copy.setReferencedAssign(context.getAssgin(target));
        }
        else
        {
            copy.setReferencedAssign(m_assign_impl);
        }
        return copy;
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        super.open();
        
        // Register the referencing in the assign ops.
        m_assign_impl.addReference(this);
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        return m_assign_impl.getNext(this);
    }
}
