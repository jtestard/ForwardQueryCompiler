/**
 * 
 */
package edu.ucsd.forward.fpl.plan;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.fpl.FplInterpretationException;
import edu.ucsd.forward.fpl.ast.ActionInvocation.HttpVerb;
import edu.ucsd.forward.fpl.ast.ActionInvocation.Method;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * Represents an action call in an FPL action.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class StaticActionCall extends AbstractActionCall
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(StaticActionCall.class);
    
    private String              m_action_path;
    
    private Method              m_method;
    
    /**
     * The default constructor.
     * 
     * @param verb
     *            an HTTP verb.
     * @param method
     *            the invocation method.
     * @param new_window
     *            whether the action result will be displayed in a new browser window.
     * @param action_path
     *            the action path.
     * @param arguments
     *            the action call arguments.
     */
    public StaticActionCall(HttpVerb verb, Method method, boolean new_window, String action_path, List<PhysicalPlan> arguments)
    {
        super(verb, new_window, arguments);
        assert (action_path != null);
        assert method != null;
        m_method = method;
        m_action_path = action_path;
    }
    
    /**
     * The default constructor.
     * 
     * @param verb
     *            an HTTP verb.
     * @param new_window
     *            whether the action result will be displayed in a new browser window.
     * @param action_path
     *            the action path.
     * @param arguments
     *            the action call arguments.
     */
    public StaticActionCall(HttpVerb verb, boolean new_window, String action_path, List<PhysicalPlan> arguments)
    {
        this(verb, Method.SYNC, new_window, action_path, arguments);
    }
    
    /**
     * Gets the action path.
     * 
     * @return the action path.
     */
    public String getActionPath()
    {
        return m_action_path;
    }
    
    @Override
    public String toExplainString()
    {
        String str = super.toExplainString();
        str += this.getHttpVerb().name() + " ";
        if (getMethod() == Method.ASYNC)
        {
            str += "ASYNC ";
        }
        if (this.inNewWindow())
        {
            str += "IN NEW ";
        }
        str += m_action_path;
        str += "(";
        int count = 0;
        for (PhysicalPlan arg : this.getCallArguments())
        {
            str += arg.toExplainString();
            if (++count < this.getCallArguments().size())
            {
                str += ", ";
            }
        }
        str += ")";
        return str;
    }
    
    /**
     * Gets the invocation method.
     * 
     * @return the invocation method.
     */
    public Method getMethod()
    {
        return m_method;
    }
    
    @Override
    public String execute(UnifiedApplicationState uas) throws FplInterpretationException
    {
        super.execute(m_action_path, uas);
        
        return null;
    }
    
}
