package edu.ucsd.forward.query.physical;

import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.TupleType.AttributeEntry;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.QueryResult;
import edu.ucsd.forward.query.logical.SendPlan;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;
import edu.ucsd.forward.util.Timer;

/**
 * Represents an implementation of the send plan interface that sends a physical plan for execution to a JDBC data source.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 */
public class SendPlanImplJdbc extends AbstractSendPlanImpl
{
    private QueryResult m_query_result;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical send plan operator.
     */
    public SendPlanImplJdbc(SendPlan logical)
    {
        super(logical);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        super.open();
        
        m_query_result = null;
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        if (m_query_result == null)
        {
            // Execute the copy operator implementation children
            super.next();
            
            QueryProcessor processor = QueryProcessorFactory.getInstance();
            m_query_result = processor.createLazyQueryResult(this.getSendPlan(), processor.getUnifiedApplicationState());
            
            // Open the query result
            m_query_result.open();
        }
        
        long time = System.currentTimeMillis();
        
        Binding next = m_query_result.next();
        
        // Check if tuple values need to be reconstructed
        if (next != null)
        {
            Binding compact_next = new Binding();
            int index = 0;
            for (RelativeVariable var : this.getOperator().getSendPlan().getRootOperator().getOutputInfo().getVariables())
            {
                if (var.getType() instanceof TupleType)
                {
                    TupleValue tuple_value = new TupleValue();
                    for (AttributeEntry entry : ((TupleType) var.getType()))
                    {
                        tuple_value.setAttribute(entry.getName(), next.getValue(index).getValue());
                        index++;
                    }
                    compact_next.addValue(new BindingValue(tuple_value, true));
                }
                else
                {
                    compact_next.addValue(next.getValue(index));
                    index++;
                }
            }
            next = compact_next;
        }
        
        Timer.inc("JdbcSendPlanNext", System.currentTimeMillis() - time);
        
        // if (next != null && this.getParent() == null)
        // {
        // Timer.inc("Tuples Produced", 1);
        // }
        
        return next;
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        if (this.getState() == State.OPEN)
        {
            // Close the query result
            if (m_query_result != null) m_query_result.close();
        }
        
        super.close();
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        SendPlanImplJdbc copy = new SendPlanImplJdbc(this.getOperator());
        
        // We need to copy the physical plan, even though it will be translated to SQL, because of different parameter
        // instantiations per plan execution.
        copy.setSendPlan(this.getSendPlan().copy(context));
        
        for (OperatorImpl child : this.getChildren())
        {
            copy.addChild(child.copy(context));
        }
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitSendPlanImplJdbc(this);
    }
    
}
