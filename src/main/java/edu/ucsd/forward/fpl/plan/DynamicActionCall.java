/**
 * 
 */
package edu.ucsd.forward.fpl.plan;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.ExceptionMessages;
import edu.ucsd.forward.fpl.FplInterpretationException;
import edu.ucsd.forward.fpl.ast.ActionInvocation.HttpVerb;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * Represents an action call in an FPL action.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class DynamicActionCall extends AbstractActionCall
{
    @SuppressWarnings("unused")
    private static final Logger log  = Logger.getLogger(DynamicActionCall.class);
    
    private static final String NAME = "call";
    
    private PhysicalPlan        m_action_path;
    
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
    public DynamicActionCall(HttpVerb verb, boolean new_window, PhysicalPlan action_path, List<PhysicalPlan> arguments)
    {
        super(verb, new_window, arguments);
        assert (action_path != null);
        m_action_path = action_path;
    }
    
    /**
     * Gets the action path.
     * 
     * @return the action path.
     */
    public PhysicalPlan getActionPath()
    {
        return m_action_path;
    }
    
    @Override
    public String toExplainString()
    {
        String str = super.toExplainString();
        str += this.getHttpVerb().name() + " ";
        if (this.inNewWindow())
        {
            str += "IN NEW ";
        }
        str += m_action_path.toExplainString();
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
    
    @Override
    public String execute(UnifiedApplicationState uas) throws FplInterpretationException
    {
        // Evaluate the action path
        Value action_path_value = QueryProcessorFactory.getInstance().createEagerQueryResult(this.getActionPath(), uas).getValue();
        
        // Handle NULL argument
        if (action_path_value instanceof NullValue)
        {
            throw new QueryExecutionException(ExceptionMessages.QueryExecution.FUNCTION_EVAL_NULL_INPUT_ERROR, NAME);
        }
        
        String target_action_path = ((StringValue) action_path_value).toString();
        
        super.execute(target_action_path, uas);
        
        return null;
    }
}
