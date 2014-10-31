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
 * The declaration of a user-defined exception.
 * 
 * @author Yupeng
 * 
 */
public class ExceptionDeclaration extends AbstractDeclaration
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ExceptionDeclaration.class);
    
    private String              m_exception_name;
    
    /**
     * Constructor.
     * 
     * @param location
     *            a location.
     */
    public ExceptionDeclaration(Location location)
    {
        super(location);
    }
    
    /**
     * Sets the exception name.
     * 
     * @param exception_name
     *            the name of the declared exception.
     */
    public void setExceptionName(String exception_name)
    {
        assert exception_name != null;
        m_exception_name = exception_name;
    }
    
    /**
     * Gets the name of the declared exception.
     * 
     * @return the name of the declared exception.
     */
    public String getExceptionName()
    {
        return m_exception_name;
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws FplCompilationException
    {
        return visitor.visitExceptionDeclaration(this);
    }
    
    @Override
    public AstNode copy()
    {
        ExceptionDeclaration copy = new ExceptionDeclaration(this.getLocation());
        super.copy(copy);
        copy.setExceptionName(getExceptionName());
        
        return copy;
    }
    
    @Override
    public void toActionString(StringBuilder sb, int tabs)
    {
        sb.append(this.getIndent(tabs) + getExceptionName() + " EXCEPTION" + ";" + FplPrinter.NL);
    }
}
