/**
 * 
 */
package edu.ucsd.forward.fpl.ast;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.fpl.FplCompilationException;
import edu.ucsd.forward.fpl.explain.FplPrinter;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * The declaration section of the function.
 * 
 * @author Yupeng
 * 
 */
public class DeclarationSection extends AbstractFplConstruct
{
    @SuppressWarnings("unused")
    private static final Logger        log = Logger.getLogger(DeclarationSection.class);
    
    private List<VariableDeclaration>  m_variable_declaration;
    
    private List<ExceptionDeclaration> m_exception_declaration;
    
    /**
     * Default constructor.
     * 
     * @param location
     *            a location.
     */
    public DeclarationSection(Location location)
    {
        super(location);
        
        m_exception_declaration = new ArrayList<ExceptionDeclaration>();
        m_variable_declaration = new ArrayList<VariableDeclaration>();
        
    }
    
    /**
     * Adds a variable declaration.
     * 
     * @param declaration
     *            the variable declaration to add.
     */
    public void addVariableDeclaration(VariableDeclaration declaration)
    {
        assert declaration != null;
        m_variable_declaration.add(declaration);
        this.addChild(declaration);
    }
    
    /**
     * Adds an exception declaration.
     * 
     * @param declaration
     *            the exception declaration to add.
     */
    public void addExceptionDeclaration(ExceptionDeclaration declaration)
    {
        assert declaration != null;
        m_exception_declaration.add(declaration);
        this.addChild(declaration);
    }
    
    /**
     * Gets the variable declarations.
     * 
     * @return the variable declarations.
     */
    public List<VariableDeclaration> getVariableDeclarations()
    {
        return new ArrayList<VariableDeclaration>(m_variable_declaration);
    }
    
    /**
     * Gets the exception declarations.
     * 
     * @return the exception declarations.
     */
    public List<ExceptionDeclaration> getExceptionDeclarations()
    {
        return new ArrayList<ExceptionDeclaration>(m_exception_declaration);
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws FplCompilationException
    {
        return visitor.visitFunctionDeclarationSection(this);
    }
    
    @Override
    public AstNode copy()
    {
        DeclarationSection copy = new DeclarationSection(this.getLocation());
        super.copy(copy);
        for (VariableDeclaration declaration : getVariableDeclarations())
        {
            copy.addVariableDeclaration((VariableDeclaration) declaration.copy());
        }
        for (ExceptionDeclaration declaration : getExceptionDeclarations())
        {
            copy.addExceptionDeclaration((ExceptionDeclaration) declaration.copy());
        }
        return copy;
    }
    
    @Override
    public void toActionString(StringBuilder sb, int tabs)
    {
        sb.append(getIndent(tabs) + "DECLARE" + FplPrinter.NL);
        for (VariableDeclaration declaration : getVariableDeclarations())
        {
            declaration.toActionString(sb, tabs + 1);
        }
        for (ExceptionDeclaration declaration : getExceptionDeclarations())
        {
            declaration.toActionString(sb, tabs + 1);
        }
    }
}
