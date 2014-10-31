/**
 * 
 */
package edu.ucsd.forward.query;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A factory that produces the query processor. There is singleton query processor instance per thread.
 * 
 * @author Yupeng
 * 
 */
public final class QueryProcessorFactory
{
    @SuppressWarnings("unused")
    private static final Logger                      log         = Logger.getLogger(QueryProcessorFactory.class);
    
    /**
     * The singleton query processor instance per thread.
     */
    private static final ThreadLocal<QueryProcessor> QP_INSTANCE = new ThreadLocal<QueryProcessor>();
    
    /**
     * Hidden constructor.
     */
    private QueryProcessorFactory()
    {
        
    }
    
    /**
     * Gets the singleton query processor instance.
     * 
     * @return the singleton query processor instance.
     */
    public static QueryProcessor getInstance()
    {
        QueryProcessor query_processor = QP_INSTANCE.get();
        if (query_processor == null)
        {
            query_processor = new QueryProcessor();
            QP_INSTANCE.set(query_processor);
        }
        
        return query_processor;
    }
}
