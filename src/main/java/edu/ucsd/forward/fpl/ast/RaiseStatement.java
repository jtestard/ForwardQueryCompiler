/**
 * 
 */
package edu.ucsd.forward.fpl.ast;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.fpl.FplCompilationException;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * The RAISE statement stops normal execution of a function and transfers control to an exception handler.
 * 
 * RAISE statements can raise predefined exceptions, such as ZERO_DIVIDE or NO_DATA_FOUND, or user-defined exceptions.
 * 
 * @author Yupeng
 * 
 */
public class RaiseStatement extends AbstractStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(RaiseStatement.class);
    
    private String              m_exception_name;
    
    /**
     * Constructor.
     * 
     * @param location
     *            a location.
     */
    public RaiseStatement(Location location)
    {
        super(location);
    }
    
    /**
     * Gets the exception name to raise.
     * 
     * @return the exception name to raise.
     */
    public String getExceptionName()
    {
        return m_exception_name;
    }
    
    /**
     * Sets the exception name to raise.
     * 
     * @param exception_name
     *            the exception name to raise.
     */
    public void setExceptionName(String exception_name)
    {
        assert exception_name != null;
        m_exception_name = exception_name;
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws FplCompilationException
    {
        return visitor.visitRaiseStatement(this);
    }
    
    @Override
    public AstNode copy()
    {
        RaiseStatement copy = new RaiseStatement(this.getLocation());
        super.copy(copy());
        copy.setExceptionName(getExceptionName());
        return copy;
    }
    
    @Override
    public void toActionString(StringBuilder sb, int tabs)
    {
        sb.append(this.getIndent(tabs) + "RAISE " + getExceptionName());
    }
}
