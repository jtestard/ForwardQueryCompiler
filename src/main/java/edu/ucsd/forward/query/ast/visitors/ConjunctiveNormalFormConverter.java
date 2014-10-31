package edu.ucsd.forward.query.ast.visitors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.AttributeReference;
import edu.ucsd.forward.query.ast.DynamicParameter;
import edu.ucsd.forward.query.ast.ExistsNode;
import edu.ucsd.forward.query.ast.FlattenItem;
import edu.ucsd.forward.query.ast.FromExpressionItem;
import edu.ucsd.forward.query.ast.GroupByItem;
import edu.ucsd.forward.query.ast.JoinItem;
import edu.ucsd.forward.query.ast.OptionItem;
import edu.ucsd.forward.query.ast.OrderByItem;
import edu.ucsd.forward.query.ast.QueryConstruct;
import edu.ucsd.forward.query.ast.QueryExpression;
import edu.ucsd.forward.query.ast.QuerySpecification;
import edu.ucsd.forward.query.ast.SelectAllItem;
import edu.ucsd.forward.query.ast.SelectExpressionItem;
import edu.ucsd.forward.query.ast.SetOpExpression;
import edu.ucsd.forward.query.ast.SwitchNode;
import edu.ucsd.forward.query.ast.TupleAllItem;
import edu.ucsd.forward.query.ast.TupleItem;
import edu.ucsd.forward.query.ast.ValueExpression;
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
import edu.ucsd.forward.query.ast.function.FunctionNode;
import edu.ucsd.forward.query.ast.function.GeneralFunctionNode;
import edu.ucsd.forward.query.ast.function.TupleFunctionNode;
import edu.ucsd.forward.query.ast.literal.BooleanLiteral;
import edu.ucsd.forward.query.ast.literal.NullLiteral;
import edu.ucsd.forward.query.ast.literal.NumericLiteral;
import edu.ucsd.forward.query.ast.literal.StringLiteral;
import edu.ucsd.forward.query.function.logical.AndFunction;
import edu.ucsd.forward.query.function.logical.NotFunction;
import edu.ucsd.forward.query.function.logical.OrFunction;

/**
 * The AST node visitor that converts a boolean expression into conjunctive normal form (CNF) by repeated application of
 * equivalences. The visitor first move NOTs inwards by repeatedly applying DeMorgan's Law, and then distributes ORs over ANDs.
 * 
 * @author Michalis Petropoulos
 * 
 *         This visitor is based on the CNFConverter.java found at:
 *         http://aima-java.googlecode.com/svn/trunk/aima-core/src/main/java/aima/core/logic/fol/CNFConverter.java.
 * 
 *         The above implementation is part of the AIMA project that provides Java implementation of the algorithms from Norvig And
 *         Russell's "Artificial Intelligence - A Modern Approach 3rd Edition." The web page of the project is:
 *         http://code.google.com/p/aima-java/
 */
public final class ConjunctiveNormalFormConverter
{
    /**
     * Hidden constructor.
     */
    private ConjunctiveNormalFormConverter()
    {
    }
    
    /**
     * Converts the input AST subtree into conjunctive normal form (CNF).
     * 
     * @param ast_node
     *            the root of the AST subtree to convert.
     * @return an AST subtree in CNF.
     * @throws QueryCompilationException
     *             when there are more than one suffix matches.
     */
    public static AstNode convert(AstNode ast_node) throws QueryCompilationException
    {
        assert (ast_node != null);
        
        ConjunctiveNormalFormConverter converter = new ConjunctiveNormalFormConverter();
        
        // Copy
        QueryConstruct copy = (QueryConstruct) ast_node.copy();
        
        // Move NOTs inwards.
        QueryConstruct negations_in = (QueryConstruct) copy.accept(converter.new NegationsIn());
        
        // Distribute ORs over ANDs.
        AstNode or_distributed_over_and = (AstNode) negations_in.accept(converter.new DistributeOrOverAnd());
        
        return or_distributed_over_and;
    }
    
    /**
     * Extracts the conjuncts from the input expression in CNF.
     * 
     * @param expr
     *            an expression in CNF.
     * @return a list of conjuncts.
     */
    public static List<ValueExpression> extractConjuncts(ValueExpression expr)
    {
        List<ValueExpression> conjuncts = new ArrayList<ValueExpression>();
        
        if (isAndFunction(expr))
        {
            for (ValueExpression arg : ((FunctionNode) expr).getArguments())
            {
                if (isAndFunction(arg))
                {
                    conjuncts.addAll(extractConjuncts((FunctionNode) arg));
                }
                else
                {
                    conjuncts.add(arg);
                }
            }
        }
        else
        {
            conjuncts.add(expr);
        }
        
        return conjuncts;
    }
    
    /**
     * Extracts the disjuncts from the input expression in DNF.
     * 
     * @param expr
     *            an expression in DNF.
     * @return a list of disjuncts.
     */
    public static List<ValueExpression> extractDisjuncts(ValueExpression expr)
    {
        List<ValueExpression> disjuncts = new ArrayList<ValueExpression>();
        
        if (isOrFunction(expr))
        {
            for (ValueExpression arg : ((FunctionNode) expr).getArguments())
            {
                if (isOrFunction(arg))
                {
                    disjuncts.addAll(extractDisjuncts((FunctionNode) arg));
                }
                else
                {
                    disjuncts.add(arg);
                }
            }
        }
        else
        {
            disjuncts.add(expr);
        }
        
        return disjuncts;
    }
    
    /**
     * Determines if the input AST node is an AND function.
     * 
     * @param ast_node
     *            an AST node.
     * @return true, if the input AST node is an AND function; false, otherwise.
     */
    protected static boolean isAndFunction(AstNode ast_node)
    {
        if (ast_node instanceof FunctionNode && ((FunctionNode) ast_node).getFunctionName().equals(AndFunction.NAME))
        {
            return true;
        }
        
        return false;
    }
    
    /**
     * Determines if the input AST node is an OR function.
     * 
     * @param ast_node
     *            an AST node.
     * @return true, if the input AST node is an OR function; false, otherwise.
     */
    protected static boolean isOrFunction(AstNode ast_node)
    {
        if (ast_node instanceof FunctionNode && ((FunctionNode) ast_node).getFunctionName().equals(OrFunction.NAME))
        {
            return true;
        }
        
        return false;
    }
    
    /**
     * Determines if the input AST node is a NOT function.
     * 
     * @param ast_node
     *            an AST node.
     * @return true, if the input AST node is a NOT function; false, otherwise.
     */
    protected static boolean isNotFunction(AstNode ast_node)
    {
        if (ast_node instanceof FunctionNode && ((FunctionNode) ast_node).getFunctionName().equals(NotFunction.NAME))
        {
            return true;
        }
        
        return false;
    }
    
    /**
     * Move NOTs inwards by repeatedly applying DeMorgan's Law.
     * 
     * @author Michalis Petropoulos
     * 
     */
    class NegationsIn extends AbstractQueryAstNodeVisitor
    {
        @Override
        public AstNode visitAggregateFunctionNode(AggregateFunctionNode function_node)
        {
            return function_node;
        }
        
        @Override
        public AstNode visitAttributeReference(AttributeReference attr_ref)
        {
            return attr_ref;
        }
        
        @Override
        public AstNode visitBooleanLiteral(BooleanLiteral boolean_literal)
        {
            return boolean_literal;
        }
        
        @Override
        public AstNode visitCaseFunctionNode(CaseFunctionNode function_node)
        {
            return function_node;
        }
        
        @Override
        public AstNode visitCastFunctionNode(CastFunctionNode function_node)
        {
            return function_node;
        }
        
        @Override
        public AstNode visitExistsNode(ExistsNode ast_node)
        {
            return ast_node;
        }
        
        @Override
        public AstNode visitExternalFunctionNode(ExternalFunctionNode ast_node)
        {
            return ast_node;
        }
        
        @Override
        public AstNode visitFromExpressionItem(FromExpressionItem from_expr_item)
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public AstNode visitGroupByItem(GroupByItem group_by_item)
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public AstNode visitJoinItem(JoinItem join_item)
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public AstNode visitNullLiteral(NullLiteral ast_node)
        {
            return ast_node;
        }
        
        @Override
        public AstNode visitNumericLiteral(NumericLiteral numeric_literal)
        {
            return numeric_literal;
        }
        
        @Override
        public AstNode visitOptionItem(OptionItem ast_node)
        {
            return ast_node;
        }
        
        @Override
        public AstNode visitOrderByItem(OrderByItem order_by_item)
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public AstNode visitParameter(DynamicParameter parameter)
        {
            return parameter;
        }
        
        @Override
        public AstNode visitQueryExpression(QueryExpression query_expr)
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public AstNode visitQuerySpecification(QuerySpecification query_spec)
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public AstNode visitGeneralFunctionNode(GeneralFunctionNode function_node) throws QueryCompilationException
        {
            if (isNotFunction(function_node))
            {
                return visitNotFunctionCall(function_node);
            }
            else if (isAndFunction(function_node) || isOrFunction(function_node))
            {
                QueryConstruct left = function_node.getArguments().get(0);
                QueryConstruct right = function_node.getArguments().get(1);
                
                // Remove arguments before visiting.
                function_node.removeArguments();
                
                left = (QueryConstruct) left.accept(this);
                right = (QueryConstruct) right.accept(this);
                
                function_node.addArgument((ValueExpression) left);
                function_node.addArgument((ValueExpression) right);
                
                return function_node;
            }
            else
            {
                // For functions other than AND, OR and NOT, simply return the function call and leave them intact.
                return function_node;
            }
        }
        
        /**
         * Moves NOTs inwards by repeated application of equivalences, since CNF requires NOTs to appear only in predicates.
         * 
         * @param function_node
         *            a NOT function node.
         * @return an AST node.
         * @throws QueryCompilationException
         *             when there are more than one suffix matches.
         */
        private AstNode visitNotFunctionCall(FunctionNode function_node) throws QueryCompilationException
        {
            assert (isNotFunction(function_node));
            
            // Get the only argument of the NOT function.
            QueryConstruct negated_node = function_node.getArguments().get(0);
            // Remove arguments before visiting.
            function_node.removeArguments();
            
            // Equivalence #1: NOT(NOT(expr)) equivalent to expr (Double Negation Elimination)
            if (isNotFunction(negated_node))
            {
                QueryConstruct double_negated_node = ((FunctionNode) negated_node).getArguments().get(0);
                // Remove arguments before visiting.
                ((FunctionNode) negated_node).removeArguments();
                
                return (AstNode) double_negated_node.accept(this);
            }
            
            if (isAndFunction(negated_node) || isOrFunction(negated_node))
            {
                FunctionNode negated_logical = (FunctionNode) negated_node;
                AstNode left = negated_logical.getArguments().get(0);
                AstNode right = negated_logical.getArguments().get(1);
                // Remove arguments before visiting.
                negated_logical.removeArguments();
                
                // Make sure that NOTs are moved in deeper
                left = (AstNode) (new GeneralFunctionNode(NotFunction.NAME, Collections.singletonList((ValueExpression) left),
                                                          left.getLocation())).accept(this);
                right = (AstNode) (new GeneralFunctionNode(NotFunction.NAME, Collections.singletonList((ValueExpression) right),
                                                           right.getLocation())).accept(this);
                
                // Equivalence #2: NOT(expr1 AND expr2) equivalent to NOT(expr1) OR NOT(expr2) (DeMorgan's Law)
                if (isAndFunction(negated_logical))
                {
                    ValueExpression[] args = { (ValueExpression) left, (ValueExpression) right };
                    return new GeneralFunctionNode(OrFunction.NAME, Arrays.asList(args), left.getLocation());
                }
                
                // Equivalence #3: NOT(expr1 OR expr2) equivalent to NOT(expr1) AND NOT(expr2) (DeMorgan's Law)
                if (isOrFunction(negated_logical))
                {
                    ValueExpression[] args = { (ValueExpression) left, (ValueExpression) right };
                    return new GeneralFunctionNode(AndFunction.NAME, Arrays.asList(args), left.getLocation());
                }
            }
            
            AstNode visited_negated_expr = (AstNode) negated_node.accept(this);
            function_node.addArgument((ValueExpression) visited_negated_expr);
            
            return function_node;
        }
        
        @Override
        public AstNode visitSelectAllItem(SelectAllItem all_item)
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public AstNode visitSelectExpressionItem(SelectExpressionItem expr_item)
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public AstNode visitSetOpExpression(SetOpExpression set_op_expr)
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public AstNode visitStringLiteral(StringLiteral string_literal)
        {
            return string_literal;
        }
        
        @Override
        public AstNode visitSwitchNode(SwitchNode ast_node)
        {
            return ast_node;
        }
        
        @Override
        public AstNode visitTupleFunctionNode(TupleFunctionNode ast_node)
        {
            return ast_node;
        }
        
        @Override
        public AstNode visitTupleItem(TupleItem ast_node)
        {
            return ast_node;
        }
        
        @Override
        public AstNode visitDeleteStatement(DeleteStatement ast_node)
        {
            return ast_node;
        }
        
        @Override
        public AstNode visitInsertStatement(InsertStatement ast_node)
        {
            
            return ast_node;
        }
        
        @Override
        public AstNode visitUpdateStatement(UpdateStatement ast_node)
        {
            return ast_node;
        }
        
        @Override
        public AstNode visitAccessStatement(AccessStatement ast_node)
        {
            return ast_node;
        }
        
        @Override
        public Object visitCreateStatement(CreateStatement ast_node) throws QueryCompilationException
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public Object visitDropStatement(DropStatement ast_node) throws QueryCompilationException
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public Object visitWithItem(WithItem ast_node) throws QueryCompilationException
        {
            return ast_node;
        }
        
        @Override
        public Object visitTupleAllItem(TupleAllItem ast_node) throws QueryCompilationException
        {
            // This should not be called.
            throw new AssertionError();
        }

        @Override
        public Object visitCollectionFunctionNode(CollectionFunctionNode ast_node) throws QueryCompilationException
        {
            return ast_node;
        }

        @Override
        public Object visitFlattenItem(FlattenItem ast_node) throws QueryCompilationException
        {
            return ast_node;
        }
        
    }
    
    /**
     * Distributes ORs over ANDs.
     * 
     * @author Michalis Petropoulos
     * 
     */
    class DistributeOrOverAnd extends AbstractQueryAstNodeVisitor
    {
        @Override
        public AstNode visitAggregateFunctionNode(AggregateFunctionNode function_node)
        {
            return function_node;
        }
        
        @Override
        public AstNode visitAttributeReference(AttributeReference attr_ref)
        {
            return attr_ref;
        }
        
        @Override
        public AstNode visitBooleanLiteral(BooleanLiteral boolean_literal)
        {
            return boolean_literal;
        }
        
        @Override
        public AstNode visitCaseFunctionNode(CaseFunctionNode function_node)
        {
            return function_node;
        }
        
        @Override
        public AstNode visitCastFunctionNode(CastFunctionNode function_node)
        {
            return function_node;
        }
        
        @Override
        public AstNode visitExistsNode(ExistsNode ast_node)
        {
            return ast_node;
        }
        
        @Override
        public AstNode visitExternalFunctionNode(ExternalFunctionNode ast_node)
        {
            return ast_node;
        }
        
        @Override
        public AstNode visitFromExpressionItem(FromExpressionItem from_expr_item)
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public AstNode visitGroupByItem(GroupByItem group_by_item)
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public AstNode visitJoinItem(JoinItem join_item)
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public AstNode visitNullLiteral(NullLiteral ast_node)
        {
            return ast_node;
        }
        
        @Override
        public AstNode visitNumericLiteral(NumericLiteral numeric_literal)
        {
            return numeric_literal;
        }
        
        @Override
        public AstNode visitOptionItem(OptionItem ast_node)
        {
            return ast_node;
        }
        
        @Override
        public AstNode visitOrderByItem(OrderByItem order_by_item)
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public AstNode visitParameter(DynamicParameter parameter)
        {
            return parameter;
        }
        
        @Override
        public AstNode visitQueryExpression(QueryExpression query_expr)
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public AstNode visitQuerySpecification(QuerySpecification query_spec)
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public AstNode visitGeneralFunctionNode(GeneralFunctionNode function_node) throws QueryCompilationException
        {
            if (isNotFunction(function_node))
            {
                QueryConstruct negated_node = function_node.getArguments().get(0);
                // Remove arguments before visiting.
                function_node.removeArguments();
                
                negated_node = (QueryConstruct) negated_node.accept(this);
                function_node.addArgument((ValueExpression) negated_node);
                
                return function_node;
            }
            else if (isOrFunction(function_node))
            {
                // Distribute ORs over ANDs
                
                // This will cause flattening out of nested ANDs and ORs
                QueryConstruct left = function_node.getArguments().get(0);
                QueryConstruct right = function_node.getArguments().get(1);
                // Remove arguments before visiting.
                function_node.removeArguments();
                
                left = (QueryConstruct) left.accept(this);
                right = (QueryConstruct) right.accept(this);
                
                // Equivalence #4: expr1 OR (expr2 AND expr3) equivalent to (expr1 OR expr2) AND (expr1 OR expr3)
                if (isAndFunction(right))
                {
                    ValueExpression and_left = ((FunctionNode) right).getArguments().get(0);
                    ValueExpression and_right = ((FunctionNode) right).getArguments().get(1);
                    ((FunctionNode) right).removeArguments();
                    
                    ValueExpression[] args1 = { (ValueExpression) left.copy(), and_left };
                    AstNode left_or = (AstNode) new GeneralFunctionNode(OrFunction.NAME, Arrays.asList(args1), left.getLocation()).accept(this);
                    ValueExpression[] args2 = { (ValueExpression) left.copy(), and_right };
                    AstNode right_or = (AstNode) new GeneralFunctionNode(OrFunction.NAME, Arrays.asList(args2), left.getLocation()).accept(this);
                    
                    ValueExpression[] args = { (ValueExpression) left_or, (ValueExpression) right_or };
                    return new GeneralFunctionNode(AndFunction.NAME, Arrays.asList(args), left_or.getLocation());
                }
                
                // Equivalence #5: (expr1 AND expr2) OR expr3 equivalent to (expr1 OR expr3) AND (expr2 OR expr3)
                if (isAndFunction(left))
                {
                    ValueExpression and_left = ((FunctionNode) left).getArguments().get(0);
                    ValueExpression and_right = ((FunctionNode) left).getArguments().get(1);
                    ((FunctionNode) left).removeArguments();
                    
                    ValueExpression[] args1 = { and_left, (ValueExpression) right.copy() };
                    AstNode left_or = (AstNode) new GeneralFunctionNode(OrFunction.NAME, Arrays.asList(args1),
                                                                        and_left.getLocation()).accept(this);
                    ValueExpression[] args2 = { and_right, (ValueExpression) right.copy() };
                    AstNode right_or = (AstNode) new GeneralFunctionNode(OrFunction.NAME, Arrays.asList(args2),
                                                                         and_right.getLocation()).accept(this);
                    
                    ValueExpression[] args = { (ValueExpression) left_or, (ValueExpression) right_or };
                    return new GeneralFunctionNode(AndFunction.NAME, Arrays.asList(args), left_or.getLocation());
                }
                
                ValueExpression[] args = { (ValueExpression) left, (ValueExpression) right };
                return new GeneralFunctionNode(OrFunction.NAME, Arrays.asList(args), left.getLocation());
            }
            else
            {
                // For functions other than AND, OR and NOT, simply return the function call and leave them intact.
                return function_node;
            }
        }
        
        @Override
        public AstNode visitSelectAllItem(SelectAllItem all_item)
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public AstNode visitSelectExpressionItem(SelectExpressionItem expr_item)
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public AstNode visitSetOpExpression(SetOpExpression set_op_expr)
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public AstNode visitStringLiteral(StringLiteral string_literal)
        {
            return string_literal;
        }
        
        @Override
        public AstNode visitSwitchNode(SwitchNode ast_node)
        {
            return ast_node;
        }
        
        @Override
        public AstNode visitTupleFunctionNode(TupleFunctionNode ast_node)
        {
            return ast_node;
        }
        
        @Override
        public AstNode visitTupleItem(TupleItem ast_node)
        {
            return ast_node;
        }
        
        @Override
        public AstNode visitDeleteStatement(DeleteStatement ast_node)
        {
            return ast_node;
        }
        
        @Override
        public AstNode visitInsertStatement(InsertStatement ast_node)
        {
            return ast_node;
        }
        
        @Override
        public AstNode visitUpdateStatement(UpdateStatement ast_node)
        {
            return ast_node;
        }
        
        @Override
        public AstNode visitAccessStatement(AccessStatement ast_node)
        {
            return ast_node;
        }
        
        @Override
        public Object visitCreateStatement(CreateStatement ast_node) throws QueryCompilationException
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public Object visitDropStatement(DropStatement ast_node) throws QueryCompilationException
        {
            // This should not be called.
            throw new AssertionError();
        }
        
        @Override
        public Object visitWithItem(WithItem ast_node) throws QueryCompilationException
        {
            return ast_node;
        }
        
        @Override
        public Object visitTupleAllItem(TupleAllItem ast_node) throws QueryCompilationException
        {
            // This should not be called.
            throw new AssertionError();
        }

        @Override
        public Object visitCollectionFunctionNode(CollectionFunctionNode ast_node) throws QueryCompilationException
        {
            return ast_node;
        }

        @Override
        public Object visitFlattenItem(FlattenItem ast_node) throws QueryCompilationException
        {
            return ast_node;
        }
        
    }
    
}
