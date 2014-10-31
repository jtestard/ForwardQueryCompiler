/**
 * 
 */
package edu.ucsd.forward.query.physical;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.QueryResult;
import edu.ucsd.forward.query.logical.SendPlan;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Represents an implementation of the send plan interface that sends a physical plan for execution to remote data source.
 * 
 * @author Yupeng
 * 
 */
public class SendPlanImplRemote extends AbstractSendPlanImpl
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(SendPlanImplRemote.class);
    
    private QueryResult         m_query_result;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical send plan operator.
     */
    public SendPlanImplRemote(SendPlan logical)
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
            
            // Lazily execute the send physical plan
            QueryProcessor processor = QueryProcessorFactory.getInstance();
            m_query_result = processor.createLazyQueryResult(this.getSendPlan(), processor.getUnifiedApplicationState());
            
            // Open the query result
            m_query_result.open();
        }
        
        Binding out_binding = m_query_result.next();
        
        return out_binding;
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        if (this.getState() == State.OPEN)
        {
            // Close the query result
            m_query_result.close();
            m_query_result = null;
        }
        super.close();
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitSendPlanImplRemote(this);
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        SendPlanImplRemote copy = new SendPlanImplRemote(this.getOperator());
        
        copy.setSendPlan(this.getSendPlan().copy(context));
        
        for (OperatorImpl child : this.getChildren())
        {
            copy.addChild(child.copy(context));
        }
        
        return copy;
    }
}
