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
 * The body of a user-defined function.
 * 
 * @author Yupeng
 * 
 */
public class Body extends AbstractFplConstruct
{
    @SuppressWarnings("unused")
    private static final Logger    log = Logger.getLogger(Body.class);
    
    private List<Statement>        m_statements;
    
    private List<ExceptionHandler> m_exception_handlers;
    
    /**
     * Default constructor.
     * 
     * @param location
     *            a location.
     */
    public Body(Location location)
    {
        super(location);
        
        m_statements = new ArrayList<Statement>();
        m_exception_handlers = new ArrayList<ExceptionHandler>();
    }
    
    /**
     * Adds a statement.
     * 
     * @param statement
     *            the statement to add.
     */
    public void addStatement(Statement statement)
    {
        assert statement != null;
        m_statements.add(statement);
        this.addChild(statement);
    }
    
    /**
     * Adds an exception handler.
     * 
     * @param exception_handler
     *            the exception handler to add.
     */
    public void addExceptionHandler(ExceptionHandler exception_handler)
    {
        assert exception_handler != null;
        m_exception_handlers.add(exception_handler);
        this.addChild(exception_handler);
    }
    
    /**
     * Gets the statements in the body.
     * 
     * @return the statements in the body.
     */
    public List<Statement> getStatements()
    {
        return new ArrayList<Statement>(m_statements);
    }
    
    /**
     * Gets the exception handlers specified in the body.
     * 
     * @return the exception handlers specified in the body.
     */
    public List<ExceptionHandler> getExceptionHandlers()
    {
        return new ArrayList<ExceptionHandler>(m_exception_handlers);
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws FplCompilationException
    {
        return visitor.visitFunctionBody(this);
    }
    
    @Override
    public AstNode copy()
    {
        Body copy = new Body(this.getLocation());
        super.copy(copy);
        for (Statement statement : getStatements())
        {
            copy.addStatement((Statement) statement.copy());
        }
        for (ExceptionHandler handler : getExceptionHandlers())
        {
            copy.addExceptionHandler((ExceptionHandler) handler.copy());
        }
        return null;
    }
    
    @Override
    public void toActionString(StringBuilder sb, int tabs)
    {
        sb.append(getIndent(tabs) + "BEGIN" + FplPrinter.NL);
        for (Statement statement : getStatements())
        {
            statement.toActionString(sb, tabs + 1);
            sb.append(";" + FplPrinter.NL);
        }
        if (!getExceptionHandlers().isEmpty())
        {
            sb.append(getIndent(tabs) + "EXCEPTION" + FplPrinter.NL);
            for (ExceptionHandler handler : getExceptionHandlers())
            {
                handler.toActionString(sb, tabs + 1);
            }
        }
        sb.append(getIndent(tabs) + "END;" + FplPrinter.NL);
    }
}
