/**
 * 
 */
package edu.ucsd.forward.fpl.ast;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.fpl.FplCompilationException;
import edu.ucsd.forward.fpl.explain.FplPrinter;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * The definition of a user-defined function.
 * 
 * @author Yupeng
 * 
 */
public class FunctionDefinition extends AbstractFplConstruct implements Definition
{
    @SuppressWarnings("unused")
    private static final Logger        log = Logger.getLogger(FunctionDefinition.class);
    
    private String                     m_name;
    
    /**
     * Indicates if the function can mutate the database state.
     */
    private boolean                    m_immutable;
    
    private Type                       m_return_type;
    
    private Body                       m_body;
    
    private DeclarationSection         m_declaration_section;
    
    private List<ParameterDeclaration> m_parameter_declaration;
    
    /**
     * Default constructor.
     * 
     * @param location
     *            a location.
     */
    public FunctionDefinition(Location location)
    {
        super(location);
        
        m_parameter_declaration = new ArrayList<ParameterDeclaration>();
    }
    
    /**
     * Sets the function name.
     * 
     * @param name
     *            the function name.
     */
    public void setName(String name)
    {
        assert name != null;
        m_name = name;
    }
    
    /**
     * Gets the function name.
     * 
     * @return the function name.
     */
    public String getName()
    {
        return m_name;
    }
    
    /**
     * Adds a parameter declaration.
     * 
     * @param declaration
     *            the parameter declaration to add.
     */
    public void addParameterDeclaration(ParameterDeclaration declaration)
    {
        assert declaration != null;
        m_parameter_declaration.add(declaration);
        this.addChild(declaration);
    }
    
    /**
     * Gets the parameter declarations.
     * 
     * @return the parameter declarations.
     */
    public List<ParameterDeclaration> getParameterDeclarations()
    {
        return new ArrayList<ParameterDeclaration>(m_parameter_declaration);
    }
    
    /**
     * Sets the function declaration section.
     * 
     * @param section
     *            the function declaration section.
     */
    public void setDeclarationSection(DeclarationSection section)
    {
        m_immutable = false;
        assert section != null;
        m_declaration_section = section;
        this.addChild(section);
    }
    
    /**
     * Sets if the function is immutable.
     * 
     * @param immutable
     *            the flag indicates if the function is immutable.
     */
    public void setImmutable(boolean immutable)
    {
        m_immutable = immutable;
    }
    
    /**
     * Checks if the function is immutable.
     * 
     * @return <code>true</code> if the function is immutable; <code>false</code> otherwise.
     */
    public boolean isImmutable()
    {
        return m_immutable;
    }
    
    /**
     * Gets the function declaration section.
     * 
     * @return the function declaration section.
     */
    public DeclarationSection getDeclarationSection()
    {
        return m_declaration_section;
    }
    
    /**
     * Sets the return type of the function.
     * 
     * @param return_type
     *            the return type of the function.
     */
    public void setReturnType(Type return_type)
    {
        m_return_type = return_type;
    }
    
    /**
     * Gets the return type of the function.
     * 
     * @return the return type of the function.
     */
    public Type getReturnType()
    {
        if (m_return_type == null) return null;
        return TypeUtil.cloneNoParent(m_return_type);
    }
    
    /**
     * Sets the function body.
     * 
     * @param body
     *            the function body.
     */
    public void setFunctionBody(Body body)
    {
        assert body != null;
        m_body = body;
        this.addChild(body);
    }
    
    /**
     * Gets the function body.
     * 
     * @return the function body.
     */
    public Body getFunctionBody()
    {
        return m_body;
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws FplCompilationException
    {
        return visitor.visitFunctionDefinition(this);
    }
    
    @Override
    public AstNode copy()
    {
        FunctionDefinition copy = new FunctionDefinition(this.getLocation());
        super.copy(copy);
        
        copy.setDeclarationSection((DeclarationSection) getDeclarationSection().copy());
        copy.setFunctionBody((Body) getFunctionBody().copy());
        copy.setImmutable(m_immutable);
        copy.setReturnType(m_return_type);
        for (ParameterDeclaration declaration : getParameterDeclarations())
        {
            copy.addParameterDeclaration((ParameterDeclaration) declaration.copy());
        }
        return copy;
    }
    
    @Override
    public void toActionString(StringBuilder sb, int tabs)
    {
        sb.append(getIndent(tabs) + "CREATE FUNCTION " + m_name);
        sb.append(getIndent(tabs) + FplPrinter.LEFT_PAREN);
        boolean first = true;
        for (ParameterDeclaration declaration : getParameterDeclarations())
        {
            if (first)
            {
                declaration.toActionString(sb, tabs);
                first = false;
            }
            else
            {
                sb.append(FplPrinter.COMMA);
                declaration.toActionString(sb, tabs);
            }
        }
        sb.append(FplPrinter.RIGHT_PAREN);
        if (m_return_type != null)
        {
            sb.append(getIndent(tabs) + "RETURNS ");
            m_return_type.toQueryString(sb, tabs, null);
        }
        sb.append(" AS" + FplPrinter.NL);
        if (m_declaration_section != null) m_declaration_section.toActionString(sb, tabs);
        m_body.toActionString(sb, tabs);
    }
}
