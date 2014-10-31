/**
 * 
 */
package edu.ucsd.forward.fpl.ast;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.fpl.FplCompilationException;
import edu.ucsd.forward.fpl.explain.FplPrinter;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * The definition of an action that associates with a function definition.
 * 
 * @author Yupeng
 * 
 */
public class ActionDefinition extends AbstractFplConstruct implements Definition
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ActionDefinition.class);
    
    private String              m_path;
    
    private String              m_function_name;
    
    /**
     * Constructor.
     * 
     * @param location
     *            a location.
     */
    public ActionDefinition(Location location)
    {
        super(location);
    }
    
    /**
     * Sets the path to the action.
     * 
     * @param path
     *            the path to the action.
     */
    public void setPath(String path)
    {
        assert path != null;
        m_path = path;
    }
    
    /**
     * Sets the name of the function that associates with the action.
     * 
     * @param function_name
     *            the name of the function that associates with the action.
     */
    public void setFunctionName(String function_name)
    {
        assert function_name != null;
        m_function_name = function_name;
    }
    
    /**
     * Gets the path to the action.
     * 
     * @return the path to the action.
     */
    public String getPath()
    {
        return m_path;
    }
    
    /**
     * Gets the name of the associated function.
     * 
     * @return the name of the associated function.
     */
    public String getFunctionName()
    {
        return m_function_name;
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws FplCompilationException
    {
        return visitor.visitActionDefinition(this);
    }
    
    @Override
    public AstNode copy()
    {
        ActionDefinition copy = new ActionDefinition(this.getLocation());
        super.copy(copy);
        copy.setPath(m_path);
        copy.setFunctionName(m_function_name);
        return copy;
    }
    
    @Override
    public void toActionString(StringBuilder sb, int tabs)
    {
        sb.append("CREATE ACTION " + m_path + " FUNCTION " + m_function_name + ";" + FplPrinter.NL);
    }
}
