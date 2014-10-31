package edu.ucsd.forward.query.ast.visitors;

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
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.AttributeReference;
import edu.ucsd.forward.query.ast.DynamicParameter;
import edu.ucsd.forward.query.ast.ExistsNode;
import edu.ucsd.forward.query.ast.FlattenItem;
import edu.ucsd.forward.query.ast.FromExpressionItem;
import edu.ucsd.forward.query.ast.GroupByItem;
import edu.ucsd.forward.query.ast.JoinItem;
import edu.ucsd.forward.query.ast.OptionItem;
import edu.ucsd.forward.query.ast.OrderByItem;
import edu.ucsd.forward.query.ast.QueryExpression;
import edu.ucsd.forward.query.ast.QuerySpecification;
import edu.ucsd.forward.query.ast.SelectAllItem;
import edu.ucsd.forward.query.ast.SelectExpressionItem;
import edu.ucsd.forward.query.ast.SetOpExpression;
import edu.ucsd.forward.query.ast.SwitchNode;
import edu.ucsd.forward.query.ast.TupleAllItem;
import edu.ucsd.forward.query.ast.TupleItem;
import edu.ucsd.forward.query.ast.WithItem;
import edu.ucsd.forward.query.ast.ddl.CreateStatement;
import edu.ucsd.forward.query.ast.ddl.DropStatement;
import edu.ucsd.forward.query.ast.dml.AccessStatement;
import edu.ucsd.forward.query.ast.dml.DeleteStatement;
import edu.ucsd.forward.query.ast.dml.InsertStatement;
import edu.ucsd.forward.query.ast.dml.UpdateStatement;
import edu.ucsd.forward.query.ast.function.AggregateFunctionNode;
import edu.ucsd.forward.query.ast.function.CaseFunctionNode;
import edu.ucsd.forward.query.ast.function.CastFunctionNode;
import edu.ucsd.forward.query.ast.function.CollectionFunctionNode;
import edu.ucsd.forward.query.ast.function.ExternalFunctionNode;
import edu.ucsd.forward.query.ast.function.GeneralFunctionNode;
import edu.ucsd.forward.query.ast.function.TupleFunctionNode;
import edu.ucsd.forward.query.ast.literal.BooleanLiteral;
import edu.ucsd.forward.query.ast.literal.NullLiteral;
import edu.ucsd.forward.query.ast.literal.NumericLiteral;
import edu.ucsd.forward.query.ast.literal.StringLiteral;

/**
 * The interface of an AST node visitor.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface AstNodeVisitor
{
    // Visit query nodes
    public Object visitAggregateFunctionNode(AggregateFunctionNode ast_node) throws QueryCompilationException;
    
    public Object visitAttributeReference(AttributeReference ast_node) throws QueryCompilationException;
    
    public Object visitBooleanLiteral(BooleanLiteral ast_node) throws QueryCompilationException;
    
    public Object visitCaseFunctionNode(CaseFunctionNode ast_node) throws QueryCompilationException;
    
    public Object visitCastFunctionNode(CastFunctionNode ast_node) throws QueryCompilationException;
    
    public Object visitCreateStatement(CreateStatement ast_node) throws QueryCompilationException;
    
    public Object visitDropStatement(DropStatement ast_node) throws QueryCompilationException;
    
    public Object visitExistsNode(ExistsNode ast_node) throws QueryCompilationException;
    
    public Object visitExternalFunctionNode(ExternalFunctionNode ast_node) throws QueryCompilationException;
    
    public Object visitFromExpressionItem(FromExpressionItem ast_node) throws QueryCompilationException;
    
    public Object visitGeneralFunctionNode(GeneralFunctionNode ast_node) throws QueryCompilationException;
    
    public Object visitGroupByItem(GroupByItem ast_node) throws QueryCompilationException;
    
    public Object visitJoinItem(JoinItem ast_node) throws QueryCompilationException;
    
    public Object visitNullLiteral(NullLiteral ast_node) throws QueryCompilationException;
    
    public Object visitNumericLiteral(NumericLiteral ast_node) throws QueryCompilationException;
    
    public Object visitOptionItem(OptionItem ast_node) throws QueryCompilationException;
    
    public Object visitOrderByItem(OrderByItem ast_node) throws QueryCompilationException;
    
    public Object visitParameter(DynamicParameter ast_node) throws QueryCompilationException;
    
    public Object visitQueryExpression(QueryExpression ast_node) throws QueryCompilationException;
    
    public Object visitQuerySpecification(QuerySpecification ast_node) throws QueryCompilationException;
    
    public Object visitSelectAllItem(SelectAllItem ast_node) throws QueryCompilationException;
    
    public Object visitSelectExpressionItem(SelectExpressionItem ast_node) throws QueryCompilationException;
    
    public Object visitSetOpExpression(SetOpExpression ast_node) throws QueryCompilationException;
    
    public Object visitStringLiteral(StringLiteral ast_node) throws QueryCompilationException;
    
    public Object visitSwitchNode(SwitchNode ast_node) throws QueryCompilationException;
    
    public Object visitTupleFunctionNode(TupleFunctionNode ast_node) throws QueryCompilationException;
    
    public Object visitCollectionFunctionNode(CollectionFunctionNode ast_node) throws QueryCompilationException;
    
    public Object visitTupleItem(TupleItem ast_node) throws QueryCompilationException;
    
    public Object visitAccessStatement(AccessStatement ast_node) throws QueryCompilationException;
    
    public Object visitInsertStatement(InsertStatement ast_node) throws QueryCompilationException;
    
    public Object visitUpdateStatement(UpdateStatement ast_node) throws QueryCompilationException;
    
    public Object visitDeleteStatement(DeleteStatement ast_node) throws QueryCompilationException;
    
    public Object visitWithItem(WithItem ast_node) throws QueryCompilationException;
    
    // Visit action nodes
    public Object visitVariableDeclaration(VariableDeclaration ast_node) throws FplCompilationException;
    
    public Object visitParameterDeclaration(ParameterDeclaration ast_node) throws FplCompilationException;
    
    public Object visitExceptionDeclaration(ExceptionDeclaration ast_node) throws FplCompilationException;
    
    public Object visitIfStatement(IfStatement ast_node) throws FplCompilationException;
    
    public Object visitRaiseStatement(RaiseStatement ast_node) throws FplCompilationException;
    
    public Object visitReturnStatement(ReturnStatement ast_node) throws FplCompilationException;
    
    public Object visitExceptionHandler(ExceptionHandler ast_node) throws FplCompilationException;
    
    public Object visitFunctionBody(Body ast_node) throws FplCompilationException;
    
    public Object visitFunctionDeclarationSection(DeclarationSection ast_node) throws FplCompilationException;
    
    public Object visitFunctionDefinition(FunctionDefinition ast_node) throws FplCompilationException;
    
    public Object visitActionDefinition(ActionDefinition ast_node) throws FplCompilationException;
    
    public Object visitBranch(Branch ast_node) throws FplCompilationException;
    
    public Object visitAssignmentStatement(AssignmentStatement ast_node) throws FplCompilationException;
    
    public Object visitActionInvocation(ActionInvocation ast_node) throws FplCompilationException;
    
    public Object visitTupleAllItem(TupleAllItem ast_node) throws QueryCompilationException;

    public Object visitFlattenItem(FlattenItem flattenItem) throws QueryCompilationException;
}
