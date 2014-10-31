package edu.ucsd.forward.query.physical;

import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.OffsetFetch;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Implementation of the offset fetch logical operator.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 */
@SuppressWarnings("serial")
public class OffsetFetchImpl extends AbstractUnaryOperatorImpl<OffsetFetch>
{
    private int m_offset;
    
    private int m_fetch;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical selection operator.
     * @param child
     *            the single child operator implementation.
     */
    public OffsetFetchImpl(OffsetFetch logical, OperatorImpl child)
    {
        super(logical);
        
        assert (child != null);
        this.addChild(child);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        super.open();
        
        m_fetch = Integer.MIN_VALUE;
        m_offset = Integer.MIN_VALUE;
        
        if (getOperator().getOffset() != null)
        {
            // Evaluate offset
            Value offset_value = TermEvaluator.evaluate(getOperator().getOffset(), new Binding()).getSqlValue(
                                                                                                              TypeEnum.INTEGER.get());
            
            if (!(offset_value instanceof NullValue))
            {
                m_offset = ((IntegerValue) offset_value).getObject();
                if (m_offset < 0)
                {
                    throw new QueryExecutionException(QueryExecution.NEGATIVE_OFFSET, m_offset);
                }
            }
        }
        
        if (getOperator().getFetch() != null)
        {
            // Evaluate fetch
            Value fetch_value = TermEvaluator.evaluate(getOperator().getFetch(), new Binding()).getSqlValue(TypeEnum.INTEGER.get());
            
            if (!(fetch_value instanceof NullValue))
            {
                m_fetch = ((IntegerValue) fetch_value).getObject();
                if (m_fetch < 0)
                {
                    throw new QueryExecutionException(QueryExecution.NEGATIVE_FETCH, m_fetch);
                }
            }
        }
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        Binding out_binding = getChild().next();
        
        while (out_binding != null)
        {
            // Skip the rows specified in offset
            if (m_offset != Integer.MIN_VALUE)
            {
                if (m_offset > 0)
                {
                    out_binding = getChild().next();
                    m_offset--;
                    continue;
                }
            }
            
            // Stop if the count is down to zero
            if (m_fetch != Integer.MIN_VALUE)
            {
                if (m_fetch > 0)
                {
                    // More to fetch
                    m_fetch--;
                    break;
                }
                else
                {
                    // Has fetched the amount that specified by the count
                    return null;
                }
            }
            // No FETCH is specified, so return the current output binding
            else
            {
                break;
            }
        }
        return out_binding;
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        OffsetFetchImpl copy = new OffsetFetchImpl(this.getOperator(), this.getChild().copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitOffsetFetchImpl(this);
    }
    
}
