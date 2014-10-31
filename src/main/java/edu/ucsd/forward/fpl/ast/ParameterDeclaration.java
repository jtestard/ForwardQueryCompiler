/**
 * 
 */
package edu.ucsd.forward.fpl.ast;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.fpl.FplCompilationException;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * The declaration of an parameter to the user created function.
 * 
 * @author Yupeng
 * 
 */
public class ParameterDeclaration extends AbstractDeclaration
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ParameterDeclaration.class);
    
    private String              m_name;
    
    private Type                m_type;
    
    /**
     * Constructor.
     * 
     * @param location
     *            a location.
     */
    public ParameterDeclaration(Location location)
    {
        super(location);
    }
    
    /**
     * Sets the parameter name.
     * 
     * @param name
     *            the parameter name.
     */
    public void setName(String name)
    {
        assert name != null;
        m_name = name;
    }
    
    /**
     * Gets the parameter name.
     * 
     * @return the parameter name.
     */
    public String getName()
    {
        return m_name;
    }
    
    /**
     * Sets the data type.
     * 
     * @param type
     *            the data type.
     */
    public void setType(Type type)
    {
        assert type != null;
        m_type = type;
    }
    
    /**
     * Gets the data type.
     * 
     * @return the data type.
     */
    public Type getType()
    {
        return TypeUtil.cloneNoParent(m_type);
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws FplCompilationException
    {
        return visitor.visitParameterDeclaration(this);
    }
    
    @Override
    public AstNode copy()
    {
        ParameterDeclaration copy = new ParameterDeclaration(this.getLocation());
        super.copy(copy);
        copy.setName(m_name);
        copy.setType(TypeUtil.clone(m_type));
        return copy;
    }
    
    @Override
    public void toActionString(StringBuilder sb, int tabs)
    {
        sb.append(getName() + " ");
        getType().toQueryString(sb, tabs, null);
    }
}
