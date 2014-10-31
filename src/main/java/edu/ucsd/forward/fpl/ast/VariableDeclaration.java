/**
 * 
 */
package edu.ucsd.forward.fpl.ast;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.fpl.FplCompilationException;
import edu.ucsd.forward.fpl.explain.FplPrinter;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * The declaration of a variable.
 * 
 * @author Yupeng
 * 
 */
public class VariableDeclaration extends AbstractDeclaration
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(VariableDeclaration.class);
    
    private String              m_name;
    
    private Type                m_type;
    
    /**
     * Constructor.
     * 
     * @param location
     *            a location.
     */
    public VariableDeclaration(Location location)
    {
        super(location);
    }
    
    /**
     * Sets the default expression of the variable.
     * 
     * @param default_expr
     *            the expression that calculates the default value of the variable.
     */
    public void setDefaultExpression(ValueExpression default_expr)
    {
        assert default_expr != null;
        addChild(default_expr);
    }
    
    /**
     * Gets the expression for computing the default value of the variable.
     * 
     * @return the expression for computing the default value of the variable.
     */
    public ValueExpression getDefaultExpression()
    {
        List<AstNode> children = this.getChildren();
        
        if (children.isEmpty()) return null;
        
        assert (children.get(0) instanceof ValueExpression);
        
        return (ValueExpression) children.get(0);
    }
    
    /**
     * Sets the variable name.
     * 
     * @param name
     *            the variable name.
     */
    public void setName(String name)
    {
        assert name != null;
        m_name = name;
    }
    
    /**
     * Gets the variable name.
     * 
     * @return the variable name.
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
        return visitor.visitVariableDeclaration(this);
    }
    
    @Override
    public AstNode copy()
    {
        VariableDeclaration copy = new VariableDeclaration(this.getLocation());
        super.copy(copy);
        copy.setName(m_name);
        copy.setType(TypeUtil.clone(m_type));
        return copy;
    }
    
    @Override
    public void toActionString(StringBuilder sb, int tabs)
    {
        sb.append(getIndent(tabs) + getName() + " ");
        getType().toQueryString(sb, tabs, null);
        if (getDefaultExpression() != null)
        {
            sb.append(" DEFAULT ");
            getDefaultExpression().toQueryString(sb, tabs, null);
        }
        sb.append(";" + FplPrinter.NL);
    }
}
