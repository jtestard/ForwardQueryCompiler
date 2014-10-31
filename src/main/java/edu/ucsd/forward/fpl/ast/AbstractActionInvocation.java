package edu.ucsd.forward.fpl.ast;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.fpl.FplCompilationException;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.ast.function.FunctionNode;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * An action call AST node, which has an HTTP verb, whether its result will be displayed in a new browser window, and action
 * arguments.
 * 
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractActionInvocation extends AbstractStatement implements ActionInvocation
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(FunctionNode.class);
    
    private HttpVerb            m_verb;
    
    private Method              m_method;
    
    private boolean             m_new_window;
    
    /**
     * Constructs the action call.
     * 
     * @param verb
     *            an HTTP verb.
     * @param method
     *            the invocation method
     * @param new_window
     *            whether the action result will be displayed in a new browser window.
     * @param args
     *            the invocation arguments.
     * @param location
     *            a location.
     */
    protected AbstractActionInvocation(HttpVerb verb, Method method, boolean new_window, List<ValueExpression> args,
            Location location)
    {
        super(location);
        
        assert (verb != null && args != null);
        m_verb = verb;
        assert method != null;
        m_method = method;
        m_new_window = new_window;
        for (AstNode arg : args)
            this.addChild(arg);
    }
    
    @Override
    public HttpVerb getHttpVerb()
    {
        return m_verb;
    }
    
    @Override
    public Method getMethod()
    {
        return m_method;
    }
    
    @Override
    public boolean inNewWindow()
    {
        return m_new_window;
    }
    
    @Override
    public List<ValueExpression> getInvocationArguments()
    {
        List<ValueExpression> out = new ArrayList<ValueExpression>();
        for (AstNode arg : this.getChildren())
            out.add((ValueExpression) arg);
        
        return out;
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws FplCompilationException
    {
        return visitor.visitActionInvocation(this);
    }
    
}
