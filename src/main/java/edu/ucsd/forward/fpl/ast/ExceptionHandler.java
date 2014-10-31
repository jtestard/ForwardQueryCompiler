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
 * An exception handler processes a raised exception (run-time error or warning condition). The exception can be either predefined
 * or user-defined.
 * 
 * @author Yupeng
 * 
 */
public class ExceptionHandler extends AbstractFplConstruct
{
    
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ExceptionHandler.class);
    
    /**
     * Indicates the names of the raised exceptions to process. When it is empty, it indicates OTHERS.
     */
    private List<String>        m_exception_names;
    
    /**
     * Constructs the exception handler.
     * 
     * @param location
     *            a location.
     */
    public ExceptionHandler(Location location)
    {
        super(location);
        
        m_exception_names = new ArrayList<String>();
    }
    
    /**
     * Adds an exception name to handle.
     * 
     * @param exception_name
     *            an exception name to handle.
     */
    public void addExceptionName(String exception_name)
    {
        assert exception_name != null;
        m_exception_names.add(exception_name);
    }
    
    /**
     * Gets the names of the raised exceptions to process.
     * 
     * @return the names of the raised exceptions to process.
     */
    public List<String> getExceptionNames()
    {
        return m_exception_names;
    }
    
    /**
     * Gets the statements.
     * 
     * @return the statements.
     */
    public List<Statement> getStatements()
    {
        List<Statement> children = new ArrayList<Statement>();
        for (AstNode arg : this.getChildren())
        {
            children.add((Statement) arg);
        }
        
        return children;
    }
    
    /**
     * Adds a statement.
     * 
     * @param statement
     *            the statement to add
     */
    public void addStatement(Statement statement)
    {
        assert statement != null;
        this.addChild(statement);
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws FplCompilationException
    {
        return visitor.visitExceptionHandler(this);
    }
    
    @Override
    public AstNode copy()
    {
        ExceptionHandler copy = new ExceptionHandler(this.getLocation());
        super.copy(copy);
        for (String exception_name : getExceptionNames())
        {
            copy.addExceptionName(exception_name);
        }
        for (Statement statement : getStatements())
        {
            copy.addStatement((Statement) statement.copy());
        }
        return copy;
    }
    
    @Override
    public void toActionString(StringBuilder sb, int tabs)
    {
        sb.append(this.getIndent(tabs) + "WHEN ");
        if (getExceptionNames().isEmpty())
        {
            sb.append("OTHERS");
        }
        else
        {
            boolean first = true;
            for (String exception_name : getExceptionNames())
            {
                if (first)
                {
                    first = false;
                    sb.append(exception_name);
                }
                else
                {
                    sb.append(" OR " + exception_name);
                }
            }
        }
        sb.append(FplPrinter.NL + getIndent(tabs) + "THEN " + FplPrinter.NL);
        for (Statement statement : getStatements())
        {
            statement.toActionString(sb, tabs + 1);
            sb.append(";" + FplPrinter.NL);
        }
    }
}
