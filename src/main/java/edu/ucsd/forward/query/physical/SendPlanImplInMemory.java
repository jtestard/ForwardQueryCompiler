package edu.ucsd.forward.query.physical;

import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.QueryResult;
import edu.ucsd.forward.query.logical.SendPlan;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Represents an implementation of the send plan interface that sends a physical plan for execution to an in-memory data source.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 */
public class SendPlanImplInMemory extends AbstractSendPlanImpl
{
    private QueryResult m_query_result;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical send plan operator.
     */
    public SendPlanImplInMemory(SendPlan logical)
    {
        super(logical);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        super.open();
        
        // Too expensive to use during production
        // // Retrieve the data source access
        // DataSourceAccess access = QueryProcessor.getInstance().getDataSourceAccess(getOperator().getExecutionDataSourceName());
        // StorageSystem storage = access.getDataSource().getMetaData().getStorageSystem();
        // assert (storage == StorageSystem.INMEMORY);
        
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
        
        // if (out_binding != null && this.getParent() == null)
        // {
        // Timer.inc("Tuples Produced", 1);
        // }
        
        return out_binding;
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        if (this.getState() == State.OPEN && m_query_result!=null)
        {
            // Close the query result
            m_query_result.close();
        }
        
        super.close();
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        SendPlanImplInMemory copy = new SendPlanImplInMemory(this.getOperator());
        
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
        visitor.visitSendPlanImplInMemory(this);
    }
    
}
