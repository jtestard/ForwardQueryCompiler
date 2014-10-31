/**
 * 
 */
package edu.ucsd.forward.fpl;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A factory that produces the action interpreter. There is singleton action interpreter instance per thread.
 * 
 * @author Yupeng
 * 
 */
public final class FplInterpreterFactory
{
    @SuppressWarnings("unused")
    private static final Logger                         log         = Logger.getLogger(FplInterpreterFactory.class);
    
    /**
     * The singleton action interpreter instance per thread.
     */
    private static final ThreadLocal<FplInterpreter> AI_INSTANCE = new ThreadLocal<FplInterpreter>();
    
    /**
     * Hidden constructor.
     */
    private FplInterpreterFactory()
    {
        
    }
    
    /**
     * Gets the singleton action interpreter instance.
     * 
     * @return the singleton action interpreter instance.
     */
    public static FplInterpreter getInstance()
    {
        FplInterpreter action_interpreter = AI_INSTANCE.get();
        if (action_interpreter == null)
        {
            action_interpreter = new FplInterpreter();
            AI_INSTANCE.set(action_interpreter);
        }
        
        return action_interpreter;
    }
}
