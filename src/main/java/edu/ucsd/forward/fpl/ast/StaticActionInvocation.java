package edu.ucsd.forward.fpl.ast;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.fpl.ast.ActionInvocation.Method;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.ast.function.FunctionNode;

/**
 * A static action call AST node, which has a static action path.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class StaticActionInvocation extends AbstractActionInvocation
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(FunctionNode.class);
    
    private String              m_action_path;
    
    /**
     * Constructs the action call with a static path.
     * 
     * @param verb
     *            an HTTP verb.
     * @param method
     *            the invocation method
     * @param new_window
     *            whether the action result will be displayed in a new browser window.
     * @param action_path
     *            the action path.
     * @param args
     *            the invocation arguments.
     * @param location
     *            a location.
     */
    public StaticActionInvocation(HttpVerb verb, Method method, boolean new_window, String action_path, List<ValueExpression> args,
            Location location)
    {
        super(verb, method, new_window, args, location);
        assert (action_path != null);
        m_action_path = action_path;
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
    public AstNode copy()
    {
        List<ValueExpression> copy_args = new ArrayList<ValueExpression>();
        for (ValueExpression arg : this.getInvocationArguments())
            copy_args.add((ValueExpression) arg.copy());
        
        StaticActionInvocation copy = new StaticActionInvocation(this.getHttpVerb(), getMethod(), this.inNewWindow(),
                                                                 m_action_path, copy_args, this.getLocation());
        
        super.copy(copy);
        
        return copy;
    }
    
    @Override
    public void toActionString(StringBuilder sb, int tabs)
    {
        sb.append(getIndent(tabs));
        sb.append(this.getHttpVerb().name() + " ");
        if (getMethod() == Method.ASYNC)
        {
            sb.append("ASYNC ");
        }
        if (this.inNewWindow())
        {
            sb.append("IN NEW ");
        }
        sb.append(m_action_path);
        sb.append("(");
        int count = 0;
        for (AstNode arg : this.getInvocationArguments())
        {
            sb.append(arg.toString());
            if (++count < this.getInvocationArguments().size())
            {
                sb.append(", ");
            }
        }
        sb.append(")");
    }
    
}
