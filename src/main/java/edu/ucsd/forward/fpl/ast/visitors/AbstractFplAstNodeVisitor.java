/**
 * 
 */
package edu.ucsd.forward.fpl.ast.visitors;

import edu.ucsd.app2you.util.logger.Logger;
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
import edu.ucsd.forward.query.ast.visitors.AbstractAstNodeVisitor;

/**
 * The abstract implementation of AstNodeVisitor for Action AST.
 * 
 * @author Yupeng
 * 
 */
public abstract class AbstractFplAstNodeVisitor extends AbstractAstNodeVisitor
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractFplAstNodeVisitor.class);
    
    @Override
    public Object visitAccessStatement(AccessStatement ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitAggregateFunctionNode(AggregateFunctionNode ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitAttributeReference(AttributeReference ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitBooleanLiteral(BooleanLiteral ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitCaseFunctionNode(CaseFunctionNode ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitCastFunctionNode(CastFunctionNode ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitCreateStatement(CreateStatement ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitDeleteStatement(DeleteStatement ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitDropStatement(DropStatement ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitExistsNode(ExistsNode ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitExternalFunctionNode(ExternalFunctionNode ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitFromExpressionItem(FromExpressionItem ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitGeneralFunctionNode(GeneralFunctionNode ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitGroupByItem(GroupByItem ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitInsertStatement(InsertStatement ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitJoinItem(JoinItem ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitNullLiteral(NullLiteral ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitNumericLiteral(NumericLiteral ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitOptionItem(OptionItem ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitOrderByItem(OrderByItem ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitParameter(DynamicParameter ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitQueryExpression(QueryExpression ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitQuerySpecification(QuerySpecification ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitSelectAllItem(SelectAllItem ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitSelectExpressionItem(SelectExpressionItem ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitSetOpExpression(SetOpExpression ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitStringLiteral(StringLiteral ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitSwitchNode(SwitchNode ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitTupleFunctionNode(TupleFunctionNode ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitCollectionFunctionNode(CollectionFunctionNode ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitTupleItem(TupleItem ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitUpdateStatement(UpdateStatement ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitWithItem(WithItem ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    

    @Override
    public Object visitTupleAllItem(TupleAllItem ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
    
    @Override
    public Object visitFlattenItem(FlattenItem ast_node) throws QueryCompilationException
    {
        return new AssertionError();
    }
}
