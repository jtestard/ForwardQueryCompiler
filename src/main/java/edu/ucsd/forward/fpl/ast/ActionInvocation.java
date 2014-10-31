package edu.ucsd.forward.fpl.ast;

import java.util.List;

import edu.ucsd.forward.query.ast.ValueExpression;

/**
 * An action call AST node, which has an HTTP verb, whether its result will be displayed in a new browser window, an action path and
 * action arguments.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface ActionInvocation extends Statement
{
    /**
     * The types of HTTP verbs.
     * 
     * @author Michalis Petropoulos
     * 
     */
    public enum HttpVerb
    {
        GET, POST, AJAX, PUT, DELETE
    };
    
    /**
     * The types of invocation methods.
     * 
     * @author Yupeng
     * 
     */
    public enum Method
    {
        SYNC, ASYNC
    };
    
    /**
     * Gets the HTTP verb.
     * 
     * @return the HTTP verb.
     */
    public HttpVerb getHttpVerb();
    
    /**
     * Gets the invocation methods.
     * 
     * @return the invocation methods.
     */
    public Method getMethod();
    
    /**
     * Gets whether the action result will be displayed in a new browser window.
     * 
     * @return whether the action result will be displayed in a new browser window.
     */
    public boolean inNewWindow();
    
    /**
     * Gets the action invocation arguments.
     * 
     * @return the action invocation arguments.
     */
    public List<ValueExpression> getInvocationArguments();
    
}
