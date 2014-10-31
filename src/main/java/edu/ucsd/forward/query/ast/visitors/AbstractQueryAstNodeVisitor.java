/**
 * 
 */
package edu.ucsd.forward.query.ast.visitors;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.fpl.FplCompilationException;
import edu.ucsd.forward.fpl.ast.ActionDefinition;
import edu.ucsd.forward.fpl.ast.ActionInvocation;
import edu.ucsd.forward.fpl.ast.AssignmentStatement;
import edu.ucsd.forward.fpl.ast.Body;
import edu.ucsd.forward.fpl.ast.Branch;
import edu.ucsd.forward.fpl.ast.DeclarationSection;
import edu.ucsd.forward.fpl.ast.ExceptionDeclaration;
import edu.ucsd.forward.fpl.ast.ExceptionHandler;
import edu.ucsd.forward.fpl.ast.FunctionDefinition;
import edu.ucsd.forward.fpl.ast.IfStatement;
import edu.ucsd.forward.fpl.ast.ParameterDeclaration;
import edu.ucsd.forward.fpl.ast.RaiseStatement;
import edu.ucsd.forward.fpl.ast.ReturnStatement;
import edu.ucsd.forward.fpl.ast.VariableDeclaration;

/**
 * The abstract implementation of AstNodeVisitor for Query AST.
 * 
 * @author Yupeng
 * 
 */
public abstract class AbstractQueryAstNodeVisitor extends AbstractAstNodeVisitor
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractQueryAstNodeVisitor.class);
    
    @Override
    public Object visitActionDefinition(ActionDefinition ast_node) throws FplCompilationException
    {
        throw new AssertionError();
    }
    
    @Override
    public Object visitBranch(Branch ast_node) throws FplCompilationException
    {
        throw new AssertionError();
    }
    
    @Override
    public Object visitExceptionDeclaration(ExceptionDeclaration ast_node) throws FplCompilationException
    {
        throw new AssertionError();
    }
    
    @Override
    public Object visitExceptionHandler(ExceptionHandler ast_node) throws FplCompilationException
    {
        throw new AssertionError();
    }
    
    @Override
    public Object visitFunctionBody(Body ast_node) throws FplCompilationException
    {
        throw new AssertionError();
    }
    
    @Override
    public Object visitFunctionDeclarationSection(DeclarationSection ast_node) throws FplCompilationException
    {
        throw new AssertionError();
    }
    
    @Override
    public Object visitFunctionDefinition(FunctionDefinition ast_node) throws FplCompilationException
    {
        throw new AssertionError();
    }
    
    @Override
    public Object visitIfStatement(IfStatement ast_node) throws FplCompilationException
    {
        throw new AssertionError();
    }
    
    @Override
    public Object visitParameterDeclaration(ParameterDeclaration ast_node) throws FplCompilationException
    {
        throw new AssertionError();
    }
    
    @Override
    public Object visitRaiseStatement(RaiseStatement ast_node) throws FplCompilationException
    {
        throw new AssertionError();
    }
    
    @Override
    public Object visitReturnStatement(ReturnStatement ast_node) throws FplCompilationException
    {
        throw new AssertionError();
    }
    
    @Override
    public Object visitVariableDeclaration(VariableDeclaration ast_node) throws FplCompilationException
    {
        throw new AssertionError();
    }
    
    @Override
    public Object visitAssignmentStatement(AssignmentStatement ast_node) throws FplCompilationException
    {
        throw new AssertionError();
    }
    
    @Override
    public Object visitActionInvocation(ActionInvocation ast_node) throws FplCompilationException
    {
        throw new AssertionError();
    }
}
