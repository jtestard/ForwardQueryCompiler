package edu.ucsd.forward.query.ast.visitors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import edu.ucsd.app2you.util.collection.Pair;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.source.DataSourceMetaData.StorageSystem;
import edu.ucsd.forward.data.source.SchemaObject;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.SwitchType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.type.TypeException;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.exception.CheckedException;
import edu.ucsd.forward.exception.ExceptionMessages;
import edu.ucsd.forward.exception.ExceptionMessages.QueryCompilation;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.AstUtil;
import edu.ucsd.forward.query.ast.AttributeReference;
import edu.ucsd.forward.query.ast.ExistsNode;
import edu.ucsd.forward.query.ast.FlattenItem;
import edu.ucsd.forward.query.ast.FromExpressionItem;
import edu.ucsd.forward.query.ast.FromItem;
import edu.ucsd.forward.query.ast.GroupByItem;
import edu.ucsd.forward.query.ast.JoinItem;
import edu.ucsd.forward.query.ast.JoinType;
import edu.ucsd.forward.query.ast.OptionItem;
import edu.ucsd.forward.query.ast.OrderByItem;
import edu.ucsd.forward.query.ast.SwitchNode;
import edu.ucsd.forward.query.ast.OrderByItem.Nulls;
import edu.ucsd.forward.query.ast.OrderByItem.Spec;
import edu.ucsd.forward.query.ast.QueryConstruct;
import edu.ucsd.forward.query.ast.QueryExpression;
import edu.ucsd.forward.query.ast.QueryNode;
import edu.ucsd.forward.query.ast.QuerySpecification;
import edu.ucsd.forward.query.ast.QuerySpecification.SelectQualifier;
import edu.ucsd.forward.query.ast.SelectAllItem;
import edu.ucsd.forward.query.ast.SelectExpressionItem;
import edu.ucsd.forward.query.ast.SelectItem;
import edu.ucsd.forward.query.ast.SetOpExpression;
import edu.ucsd.forward.query.ast.SetOpExpression.SetOpType;
import edu.ucsd.forward.query.ast.SetQuantifier;
import edu.ucsd.forward.query.ast.TupleAllItem;
import edu.ucsd.forward.query.ast.TupleItem;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.ast.WithItem;
import edu.ucsd.forward.query.ast.ddl.CreateStatement;
import edu.ucsd.forward.query.ast.ddl.DdlStatement;
import edu.ucsd.forward.query.ast.ddl.DropStatement;
import edu.ucsd.forward.query.ast.dml.AccessStatement;
import edu.ucsd.forward.query.ast.dml.AliasedDmlStatement;
import edu.ucsd.forward.query.ast.dml.DeleteStatement;
import edu.ucsd.forward.query.ast.dml.DmlStatement;
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
import edu.ucsd.forward.query.function.Function;
import edu.ucsd.forward.query.function.FunctionRegistry;
import edu.ucsd.forward.query.function.FunctionRegistryException;
import edu.ucsd.forward.query.function.aggregate.AggregateFunction;
import edu.ucsd.forward.query.function.aggregate.AggregateFunctionCall;
import edu.ucsd.forward.query.function.cast.CastFunctionCall;
import edu.ucsd.forward.query.function.collection.CollectionFunctionCall;
import edu.ucsd.forward.query.function.comparison.IsNullFunction;
import edu.ucsd.forward.query.function.conditional.CaseFunctionCall;
import edu.ucsd.forward.query.function.external.ExternalFunction;
import edu.ucsd.forward.query.function.external.ExternalFunctionCall;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.function.logical.AndFunction;
import edu.ucsd.forward.query.function.logical.NotFunction;
import edu.ucsd.forward.query.function.logical.OrFunction;
import edu.ucsd.forward.query.function.tuple.TupleFunctionCall;
import edu.ucsd.forward.query.logical.ApplyPlan;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.logical.ConditionOperator;
import edu.ucsd.forward.query.logical.EliminateDuplicates;
import edu.ucsd.forward.query.logical.Exists;
import edu.ucsd.forward.query.logical.Ground;
import edu.ucsd.forward.query.logical.GroupBy;
import edu.ucsd.forward.query.logical.InnerJoin;
import edu.ucsd.forward.query.logical.Navigate;
import edu.ucsd.forward.query.logical.OffsetFetch;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.OuterJoin;
import edu.ucsd.forward.query.logical.OuterJoin.Variation;
import edu.ucsd.forward.query.logical.Product;
import edu.ucsd.forward.query.logical.Project;
import edu.ucsd.forward.query.logical.Project.ProjectQualifier;
import edu.ucsd.forward.query.logical.Scan.FlattenSemantics;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.Select;
import edu.ucsd.forward.query.logical.SetOperator;
import edu.ucsd.forward.query.logical.Sort;
import edu.ucsd.forward.query.logical.Subquery;
import edu.ucsd.forward.query.logical.UnaryOperator;
import edu.ucsd.forward.query.logical.ddl.CreateDataObject;
import edu.ucsd.forward.query.logical.ddl.DropDataObject;
import edu.ucsd.forward.query.logical.dml.Delete;
import edu.ucsd.forward.query.logical.dml.Insert;
import edu.ucsd.forward.query.logical.dml.Update;
import edu.ucsd.forward.query.logical.dml.Update.Assignment;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.plan.LogicalPlanUtil;
import edu.ucsd.forward.query.logical.term.AbsoluteVariable;
import edu.ucsd.forward.query.logical.term.Constant;
import edu.ucsd.forward.query.logical.term.ElementVariable;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.PositionVariable;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.util.NameGenerator;
import edu.ucsd.forward.util.NameGeneratorFactory;

/**
 * Translate the SQL++ AST to a LogicalPlan.
 * 
 * @author Romain Vernoux
 * 
 */
public final class LogicalPlanBuilder
{
    /**
     * The unified application state.
     */
    private UnifiedApplicationState          m_uas;
    
    /**
     * The context provided by the logical plan being created and the ones provided by outer logical plans.
     */
    private Stack<LogicalPlanBuilderContext> m_contexts;
    
    /**
     * A list of Assign operators (translation of the WITH clauses).
     */
    private List<Assign>                     m_assign_list          = new ArrayList<Assign>();
    
    /**
     * A stack of conditions to the execution of the branch that is currently translated.
     */
    private Stack<Term>                      m_branch_conditions    = new Stack<Term>();
    
    /**
     * If this flag is enabled, paths are translated into Navigate operators. Otherwise, they are translated into query paths. The
     * latter is needed when the Sort operator is above the Project (e.g. when the query contains a set operator), since Navigate
     * operator would introduce extra attributes in the output. For the same reasons, when this flag is enabled, nested queries are
     * not allowed.
     */
    private boolean                          m_attr_ref_to_navigate = true;
    
    /**
     * Private constructor.
     * 
     * @param contexts
     *            the context provided by the logical plan being created and the ones provided by outer logical plans..
     * @param uas
     *            the unified application state.
     * @throws QueryCompilationException
     *             if there is a query compilation error.
     */
    private LogicalPlanBuilder(Stack<LogicalPlanBuilderContext> contexts, UnifiedApplicationState uas)
            throws QueryCompilationException
    {
        assert (contexts != null);
        m_contexts = contexts;
        
        assert (uas != null);
        m_uas = uas;
    }
    
    /**
     * Translates the AST tree rooted at the input node into a tree of logical operators.
     * 
     * @param ast_node
     *            the rood node of the AST tree to translate.
     * @param uas
     *            the unified application state.
     * @return a logical plan, that is, a tree of logical operators.
     * @throws QueryCompilationException
     *             if there is a query compilation error.
     */
    public static LogicalPlan build(AstNode ast_node, UnifiedApplicationState uas) throws QueryCompilationException
    {
        return build(ast_node, new Stack<LogicalPlanBuilderContext>(), uas);
    }
    
    /**
     * Visits the AST tree rooted at the input AST node and translates it to a tree of logical operators.
     * 
     * @param ast_node
     *            the AST node to translate.
     * @param contexts
     *            the context provided by the logical plan being created and the ones provided by outer logical plans..
     * @param uas
     *            the unified application state.
     * @return a logical plan, that is, a tree of logical operators.
     * @throws QueryCompilationException
     *             if there is a query compilation error.
     */
    private static LogicalPlan build(AstNode ast_node, Stack<LogicalPlanBuilderContext> contexts, UnifiedApplicationState uas)
            throws QueryCompilationException
    {
        assert (ast_node != null);
        
        LogicalPlanBuilder converter = new LogicalPlanBuilder(contexts, uas);
        
        // Add a new context to represent the current one
        LogicalPlanBuilderContext context = new LogicalPlanBuilderContext(converter);
        converter.m_contexts.push(context);
        
        // If the ast_node is a dml statement, translate it
        if (ast_node instanceof DmlStatement || ast_node instanceof DdlStatement)
        {
            Operator root_op = converter.translateStatement((QueryConstruct) ast_node);
            
            LogicalPlan logical_plan = new LogicalPlan(root_op);
            
            logical_plan.updateOutputInfoShallow();
            
            // Remove the context created to represent the current one
            converter.m_contexts.pop();
            
            return logical_plan;
        }
        
        QueryNode root_node = null;
        boolean wrapping = false;
        
        // If this plan is wrapping (i.e. corresponds to a non-SELECT expression query), wrap it in a query specification and
        // continue
        if (ast_node instanceof ValueExpression && !(ast_node instanceof QueryNode))
        {
            // This plan is wrapping
            wrapping = true;
            
            // Wrap the AST node in a query specification
            QuerySpecification query_spec = new QuerySpecification(ast_node.getLocation());
            SelectExpressionItem item = new SelectExpressionItem((ValueExpression) ast_node.copy(), ast_node.getLocation());
            query_spec.addSelectItem(item);
            
            root_node = (QueryNode) query_spec.copy();
        }
        else
        {
            root_node = (QueryNode) ast_node.copy();
        }
        
        // Translate
        Operator root_op = converter.translateQueryNode(root_node);
        LogicalPlan logical_plan = new LogicalPlan(root_op);
        
        // Set up assign operators
        for (Assign assign : converter.m_assign_list)
        {
            logical_plan.addAssignOperator(assign);
        }
        logical_plan.updateOutputInfoShallow();
        
        // Set the wrapping flag
        logical_plan.setWrapping(wrapping);
        
        // Remove the context created to represent the current one
        converter.m_contexts.pop();
        
        return logical_plan;
    }
    
    /**
     * Translates a QueryNode.
     * 
     * @param ast_node
     *            the node to translate
     * @return the root of the translated logical plan
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Operator translateQueryNode(QueryNode ast_node) throws QueryCompilationException
    {
        // The input AST Node has to be either a QuerySpecification, a QueryExpression or a SetOpExpression
        if (ast_node instanceof QuerySpecification)
        {
            return translateQuerySpecification((QuerySpecification) ast_node);
        }
        else if (ast_node instanceof QueryExpression)
        {
            return translateQueryExpression((QueryExpression) ast_node);
        }
        else if (ast_node instanceof SetOpExpression)
        {
            return translateSetOpExpression((SetOpExpression) ast_node);
        }
        else
        {
            throw new AssertionError();
        }
    }
    
    /**
     * Translates a QueryExpression AST node.
     * 
     * @param query_expr
     *            the AST node to translate
     * @return the root of translated logical plan
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Operator translateQueryExpression(QueryExpression query_expr) throws QueryCompilationException
    {
        // The body of the query expression cannot be another query expression. This is checked in the parser.
        assert (!(query_expr.getBody() instanceof QueryExpression));
        
        // Translate the WITH items
        for (WithItem with_item : query_expr.getWithItems())
        {
            translateWithItem(with_item);
        }
        
        // The root operator of the translated logical plan
        Operator root_op;
        
        // Translate the query expression body
        QueryNode body = query_expr.getBody();
        if (body instanceof QuerySpecification)
        {
            root_op = translateQuerySpecification((QuerySpecification) body);
        }
        else if (body instanceof SetOpExpression)
        {
            root_op = translateSetOpExpression((SetOpExpression) body);
        }
        else
        {
            // Cannot happen (checked by the parser)
            throw new AssertionError();
        }
        
        // Translate ORDER BY clause
        if (query_expr.hasOrderByClause())
        {
            Sort order_by = new Sort();
            
            // If the body is a query specification, the sort operator is placed under the project operator. If the body is a set
            // expression, it is placed at the top. This is the SQL-compliant behavior.
            if (body instanceof QuerySpecification)
            {
                Operator target = root_op;
                if (target instanceof EliminateDuplicates)
                {
                    target = ((EliminateDuplicates) target).getChild();
                }
                assert (target instanceof Project);
                LogicalPlanUtil.insertOnTop(((Project) target).getChild(), order_by);
            }
            else
            {
                order_by.addChild(root_op);
                root_op = order_by;
                // The query has a set operator and therefore the OffSetFetch is above the Project
                m_attr_ref_to_navigate = false;
            }
            
            // Translate sort items.
            for (OrderByItem order_by_item : query_expr.getOrderByItems())
            {
                translateOrderByItem(order_by_item, order_by);
            }
            
            // Reset the flag
            m_attr_ref_to_navigate = true;
            
            LogicalPlanUtil.updateAncestorOutputInfo(order_by);
        }
        
        // Translate OFFSET/LIMIT clause
        ValueExpression offset_expr = query_expr.getOffset();
        ValueExpression fetch_expr = query_expr.getFetch();
        if (offset_expr != null || fetch_expr != null)
        {
            OffsetFetch offset_fetch = new OffsetFetch();
            // If the body is a query specification, the sort operator is placed under the project operator (above the Sort
            // operator, if any). If the body is a set expression, it is placed at the top (above the Sort operator, if any). This
            // is the SQL-compliant behavior.
            if (body instanceof QuerySpecification)
            {
                Operator op = root_op;
                if (op instanceof EliminateDuplicates)
                {
                    op = ((EliminateDuplicates) op).getChild();
                }
                assert (op instanceof Project);
                LogicalPlanUtil.insertOnTop(((Project) op).getChild(), offset_fetch);
            }
            else
            {
                offset_fetch.addChild(root_op);
                root_op = offset_fetch;
                // The query has a set operator and therefore the OffSetFetch is above the Project
            }
            
            // The offset and fetch terms have to be constant and therefore we don't want Navigate operators
            m_attr_ref_to_navigate = false;
            
            // Translate offset and fetch terms
            if (offset_expr != null)
            {
                translateOffset(offset_expr, offset_fetch);
            }
            if (fetch_expr != null)
            {
                translateFetch(fetch_expr, offset_fetch);
            }
            
            // Reset the flag
            m_attr_ref_to_navigate = true;
            
            LogicalPlanUtil.updateAncestorOutputInfo(offset_fetch);
        }
        
        return root_op;
    }
    
    /**
     * Translates a SetOpExpression AST node.
     * 
     * @param set_op_expr
     *            the AST node to translate
     * @return the root of translated logical plan
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private SetOperator translateSetOpExpression(SetOpExpression set_op_expr) throws QueryCompilationException
    {
        SetOperator root_op = new SetOperator(SetOpType.valueOf(set_op_expr.getSetOpType().name()), set_op_expr.getSetQuantifier());
        
        for (AstNode child : set_op_expr.getChildren())
        {
            LogicalPlan set_op_plan = build(child, m_contexts, m_uas);
            root_op.addChild(set_op_plan.getRootOperator());
        }
        
        root_op.updateOutputInfo();
        
        // Add entries to current context
        m_contexts.peek().clear();
        // Normal form assumption: the set operator outputs a single binding attribute.
        assert (root_op.getOutputInfo().getVariables().size() == 1);
        RelativeVariable var = (RelativeVariable) root_op.getOutputInfo().getVariable(0);
        AttributeReference attr_ref = new AttributeReference(Collections.singletonList(var.getName()),
                                                             new LocationImpl(Location.UNKNOWN_PATH));
        m_contexts.peek().addMappingForSuffixMatch(attr_ref, var);
        
        return root_op;
    }
    
    /**
     * Translates a QuerySpecification AST node.
     * 
     * @param query_spec
     *            the AST node to translate
     * @return the translated logical plan
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Operator translateQuerySpecification(QuerySpecification query_spec) throws QueryCompilationException
    {
        // The root operator of the translated logical plan
        Operator root_op;
        
        // Translate the FROM clause
        if (query_spec.getFromItems().isEmpty())
        {
            root_op = new Ground();
        }
        else if (query_spec.getFromItems().size() == 1)
        {
            root_op = translateFromItem(query_spec.getFromItems().get(0));
            
            Set<String> variables = new HashSet<String>();
            for (Variable var : root_op.getOutputInfo().getVariables())
            {
                assert (var instanceof RelativeVariable);
                RelativeVariable rel_var = (RelativeVariable) var;
                if (!variables.add(rel_var.getName()))
                {
                    throw new QueryCompilationException(QueryCompilation.DUPLICATE_FROM_ITEM_ALIAS, rel_var.getLocation(), rel_var);
                }
                
                // Add entries to current context
                AttributeReference attr_ref = new AttributeReference(Arrays.asList(var.getName()), var.getLocation());
                if (rel_var instanceof ElementVariable)
                {
                    // We need to copy the variable because its binding index could be different below and above a join.
                    ElementVariable copy = (ElementVariable) rel_var.copy();
                    copy.setLocation(rel_var.getLocation());
                    copy.setBindingIndex(root_op.getOutputInfo().getVariableIndex(rel_var));
                    copy.setType(rel_var.getType());
                    m_contexts.peek().addMappingForSuffixMatch(attr_ref, copy);
                }
                else if (rel_var instanceof PositionVariable)
                {
                    // We need to copy the variable because its binding index could be different below and above a join.
                    PositionVariable copy = (PositionVariable) rel_var.copy();
                    copy.setLocation(rel_var.getLocation());
                    copy.setBindingIndex(root_op.getOutputInfo().getVariableIndex(rel_var));
                    copy.setType(rel_var.getType());
                    m_contexts.peek().addMappingForExactMatch(attr_ref, copy);
                }
            }
        }
        else
        {
            root_op = new Product();
            root_op.updateOutputInfo();
            Set<String> variables = new HashSet<String>();
            
            for (FromItem from_item : query_spec.getFromItems())
            {
                Operator from = translateFromItem(from_item);
                from.updateOutputInfo();
                root_op.addChild(from);
                
                // Check for alias duplicates
                for (RelativeVariable rel_var : from.getOutputInfo().getVariables())
                {
                    if (!variables.add(rel_var.getName()))
                    {
                        throw new QueryCompilationException(QueryCompilation.DUPLICATE_FROM_ITEM_ALIAS, from_item.getLocation(),
                                                            rel_var);
                    }
                }
            }
            
            root_op.updateOutputInfo();
            
            for (RelativeVariable rel_var : root_op.getOutputInfo().getVariables())
            {
                // Add entries to current context
                AttributeReference attr_ref = new AttributeReference(Arrays.asList(rel_var.getName()), rel_var.getLocation());
                if (rel_var instanceof ElementVariable)
                {
                    m_contexts.peek().addMappingForSuffixMatch(attr_ref, rel_var);
                }
                else if (rel_var instanceof PositionVariable)
                {
                    m_contexts.peek().addMappingForExactMatch(attr_ref, rel_var);
                }
            }
        }
        root_op.updateOutputInfo();
        
        // Translate the WHERE clause
        if (query_spec.getWhereExpression() != null)
        {
            Select where_op = new Select();
            where_op.addChild(root_op);
            root_op = where_op;
            
            // Convert where condition to CNF
            
            // Romain: this is not compatible with short-circuiting
            // AstNode where = ConjunctiveNormalFormConverter.convert(query_spec.getWhereExpression());
            
            List<ValueExpression> conjuncts = ConjunctiveNormalFormConverter.extractConjuncts(query_spec.getWhereExpression());
            
            for (ValueExpression conjunct : conjuncts)
            {
                Term where_term = translateWhereItem(conjunct, where_op);
                // The next argument is only evaluated if this one is true
                m_branch_conditions.push(where_term);
            }
            for(int i = 0; i < conjuncts.size(); i++)
            {
                m_branch_conditions.pop();
            }
            where_op.updateOutputInfo();
        }
        
        // Translate the GROUP BY clause
        if (query_spec.getGroupByItems().size() > 0)
        {
            GroupBy groupby_op = new GroupBy();
            groupby_op.addChild(root_op);
            root_op = groupby_op;
            
            for (GroupByItem group : query_spec.getGroupByItems())
            {
                translateGroupByItem(group, groupby_op);
            }
            groupby_op.updateOutputInfo();
            
            // Update the current context by adding the grouping items
            for (int i = 0; i < query_spec.getGroupByItems().size(); i++)
            {
                ValueExpression value_expr = query_spec.getGroupByItems().get(i).getExpression();
                RelativeVariable variable = groupby_op.getGroupByItems().get(i).getVariable();
                variable.setLocation(value_expr.getLocation());
                variable.inferType(groupby_op.getChildren());
                m_contexts.peek().addGroupingItem(value_expr, variable);
            }
            
            // Set the flag to tell the query has a GroupBy.
            m_contexts.peek().setAboveGroupBy(true);
        }
        
        // Translate the HAVING clause
        if (query_spec.getHavingExpression() != null)
        {
            if (!(root_op instanceof GroupBy))
            {
                throw new QueryCompilationException(QueryCompilation.HAVING_CLAUSE_WITHOUT_GROUP_BY,
                                                    query_spec.getHavingExpression().getLocation());
            }
            Select having_op = new Select();
            having_op.addChild(root_op);
            root_op = having_op;
            
            // Convert having condition to CNF
            AstNode having = ConjunctiveNormalFormConverter.convert(query_spec.getHavingExpression());
            
            for (ValueExpression conjunct : ConjunctiveNormalFormConverter.extractConjuncts((ValueExpression) having))
            {
                translateHavingItem(conjunct, having_op);
            }
            
            // Before the Project, update GroupBy's output info since new aggregations and nested queries may have been added
            GroupBy groupby_op = LogicalPlanUtil.getFirstOperatorWithName(root_op, GroupBy.class);
            assert (groupby_op != null);
            LogicalPlanUtil.updateAncestorOutputInfo(groupby_op);
        }
        
        // Translate the SELECT clause
        Project project_op = new Project();
        project_op.addChild(root_op);
        root_op = project_op;
        
        // Set the SELECT ELEMENT flag
        project_op.setProjectQualifier(query_spec.getSelectQualifier() == SelectQualifier.ELEMENT
                ? ProjectQualifier.ELEMENT
                : ProjectQualifier.TUPLE);
        
        for (SelectItem select_item : query_spec.getSelectItems())
        {
            translateSelectItem(select_item, project_op);
        }
        
        LogicalPlanUtil.updateDescendentOutputInfo(project_op);
        
        // Update the current context
        RelativeVariable rel_var = (RelativeVariable) project_op.getOutputInfo().getVariable(0);
        AttributeReference attr_ref = new AttributeReference(Arrays.asList(rel_var.getName()), rel_var.getLocation());
        m_contexts.peek().addMappingForSuffixMatch(attr_ref, rel_var);
        
        // Translate the DISTINCT clause
        if (query_spec.getSetQuantifier() != null && query_spec.getSetQuantifier() == SetQuantifier.DISTINCT)
        {
            EliminateDuplicates distinct_op = new EliminateDuplicates();
            distinct_op.addChild(root_op);
            root_op = distinct_op;
            distinct_op.updateOutputInfo();
        }
        
        return root_op;
    }
    
    /**
     * Translates a DDL or DML statement into a tree of operators.
     * 
     * @param statement
     *            the statement to translate
     * @return the root of the tree of operators
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Operator translateStatement(QueryConstruct statement) throws QueryCompilationException
    {
        if (statement instanceof DmlStatement)
        {
            Ground ground = new Ground();
            ground.updateOutputInfo();
            return translateDmlStatement(statement, ground);
        }
        else if (statement instanceof DdlStatement)
        {
            if (statement instanceof CreateStatement)
            {
                return translateCreateStatement((CreateStatement) statement);
            }
            else if (statement instanceof DropStatement)
            {
                return translateDropStatement((DropStatement) statement);
            }
            else
            {
                throw new UnsupportedOperationException();
            }
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }
    
    /**
     * Translates a DML statement into a tree of operators.
     * 
     * @param statement
     *            the statement to translate
     * @param root_op
     *            the root of the current tree of operators
     * @return the root of the tree of operators
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Operator translateDmlStatement(QueryConstruct statement, Operator root_op) throws QueryCompilationException
    {
        if (statement instanceof InsertStatement)
        {
            return translateInsertStatement((InsertStatement) statement, root_op);
        }
        else if (statement instanceof DeleteStatement)
        {
            return translateDeleteStatement((DeleteStatement) statement, root_op);
        }
        else if (statement instanceof UpdateStatement)
        {
            return translateUpdateStatement((UpdateStatement) statement, root_op);
        }
        else if (statement instanceof AccessStatement)
        {
            return translateAccessStatement((AccessStatement) statement, root_op);
        }
        else
        {
            // Unsupported statement
            throw new UnsupportedOperationException();
        }
    }
    
    /**
     * Translates a CREATE statement into a tree of operators.
     * 
     * @param def_stmt
     *            the statement to translate
     * @return the corresponding operator
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private CreateDataObject translateCreateStatement(CreateStatement def_stmt) throws QueryCompilationException
    {
        CreateDataObject op = new CreateDataObject(def_stmt.getSchemaName(), def_stmt.getSchemaTree(),
                                                   def_stmt.getDataSourceName());
        for (Type type : def_stmt.getTypesToSetDefault())
        {
            SchemaPath path = new SchemaPath(type);
            // Parse the nested query
            LogicalPlan plan = LogicalPlanBuilder.build(def_stmt.getDefaultValueExpression(type), m_contexts, m_uas);
            op.addDefaultPlan(path, plan);
        }
        op.updateOutputInfo();
        
        return op;
    }
    
    /**
     * Translates a DROP statement into a LogicalPlan.
     * 
     * @param drop_stmt
     *            the statement to translate
     * @return the corresponding operator
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private DropDataObject translateDropStatement(DropStatement drop_stmt) throws QueryCompilationException
    {
        DropDataObject op = new DropDataObject(drop_stmt.getTargetName(), drop_stmt.getDataSourceName());
        op.updateOutputInfo();
        
        return op;
    }
    
    /**
     * Translates UPDATE, DELETE and ACCESS statements.
     * 
     * @param aliased_dml_stmt
     *            an UPDATE, DELETE or ACCESS statement
     * @param root_op
     *            the root of the current tree of operators
     * @return the root operator of the tree of operators
     * @throws QueryCompilationException
     *             if something goes wrong.
     */
    private Operator translateAliasedDmlStatement(AliasedDmlStatement aliased_dml_stmt, Operator root_op)
            throws QueryCompilationException
    {
        Operator op = root_op;
        if (aliased_dml_stmt.getTarget() != null)
        {
            Scan scan = new Scan(aliased_dml_stmt.getAlias());
            scan.addChild(op);
            op = scan;
            
            // Translate the target expression
            Term target_term = translateValueExpression(aliased_dml_stmt.getTarget(), scan);
            if (!(target_term.getType() instanceof CollectionType))
            {
                throw new QueryCompilationException(QueryCompilation.DML_NON_COLLECTION_TARGET_TYPE, target_term.getLocation());
            }
            scan.setTerm(target_term);
            scan.updateOutputInfo();
            
            // Add entry to current context
            RelativeVariable rel_var = scan.getAliasVariable();
            assert (!(rel_var.getType() instanceof CollectionType));
            AttributeReference attr_ref = new AttributeReference(Arrays.asList(rel_var.getName()), rel_var.getLocation());
            m_contexts.peek().addMappingForSuffixMatch(attr_ref, rel_var);
            
            // Parse condition
            if (aliased_dml_stmt.getCondition() != null)
            {
                Select where_op = new Select();
                where_op.addChild(op);
                op = where_op;
                
                // Convert where condition to CNF
                AstNode where = ConjunctiveNormalFormConverter.convert(aliased_dml_stmt.getCondition());
                
                for (ValueExpression conjunct : ConjunctiveNormalFormConverter.extractConjuncts((ValueExpression) where))
                {
                    translateWhereItem(conjunct, where_op);
                }
                
                where_op.updateOutputInfo();
            }
        }
        
        return op;
    }
    
    /**
     * Translates a DELETE statement into a tree of operators.
     * 
     * @param delete_stmt
     *            the statement to translate
     * @param root_op
     *            the root of the current tree of operators
     * @return the root of the corresponding tree of operators
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Delete translateDeleteStatement(DeleteStatement delete_stmt, Operator root_op) throws QueryCompilationException
    {
        assert (delete_stmt.getTarget() != null);
        Operator op = translateAliasedDmlStatement(delete_stmt, root_op);
        
        // Get the target data source by looking at the deepest Scan
        Collection<? extends Scan> scan_ops = op.getDescendantsAndSelf(Scan.class);
        assert (scan_ops.size() > 0);
        Scan target_scan = (Scan) scan_ops.toArray()[scan_ops.size() - 1];
        assert (target_scan != null);
        Term scan_term = target_scan.getTerm();
        
        // Make sure the scan term has one absolute variable and get its target data source
        List<AbsoluteVariable> absolute_vars = LogicalPlanUtil.getAbsoluteVariables(scan_term.getVariablesUsed());
        assert (absolute_vars.size() == 1);
        String target_data_source = absolute_vars.get(0).getDataSourceName();
        
        // Set the delete variable by looking at the top Scan
        target_scan = (Scan) scan_ops.toArray()[0];
        assert (target_scan != null);
        RelativeVariable target_rel_var = target_scan.getAliasVariable();
        
        // Get the relative variable corresponding to the target attribute reference.
        Delete delete_op = new Delete(target_data_source, scan_term, target_rel_var);
        delete_op.addChild(op);
        delete_op.updateOutputInfo();
        
        return delete_op;
    }
    
    /**
     * Translates a ACCESS statement into a tree of operators.
     * 
     * @param access_stmt
     *            the statement to translate
     * @param root_op
     *            the root of the current tree of operators
     * @return the root of the corresponding tree of operators
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Operator translateAccessStatement(AccessStatement access_stmt, Operator root_op) throws QueryCompilationException
    {
        Operator op = translateAliasedDmlStatement(access_stmt, root_op);
        
        op = translateDmlStatement(access_stmt.getStatement(), op);
        
        return op;
    }
    
    /**
     * Translates an INSERT statement into a tree of operators.
     * 
     * @param insert_stmt
     *            the statement to translate
     * @param root_op
     *            the root of the current tree of operators
     * @return the root of the corresponding tree of operators
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Operator translateInsertStatement(InsertStatement insert_stmt, Operator root_op) throws QueryCompilationException
    {
        Operator op = root_op;
        
        // Translate the attribute reference to the target collection
        assert (insert_stmt.getTarget() != null);
        String var_name = NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.VARIABLE_GENERATOR);
        Scan scan = new Scan(var_name);
        scan.addChild(op);
        op = scan;
        
        // Translate the target expression
        Term target_term = translateValueExpression(insert_stmt.getTarget(), op);
        if (!(target_term.getType() instanceof CollectionType))
        {
            throw new QueryCompilationException(QueryCompilation.DML_NON_COLLECTION_TARGET_TYPE, target_term.getLocation());
        }
        scan.setTerm(target_term);
        scan.updateOutputInfo();
        
        // Add entry to current context
        RelativeVariable rel_var = scan.getAliasVariable();
        assert (!(rel_var.getType() instanceof CollectionType));
        AttributeReference attr_ref = new AttributeReference(Arrays.asList(rel_var.getName()), rel_var.getLocation());
        m_contexts.peek().addMappingForExactMatch(attr_ref, rel_var);
        
        // Get the target data source using the descendant scan operators
        Collection<? extends Scan> scan_ops = op.getDescendantsAndSelf(Scan.class);
        assert (scan_ops.size() > 0);
        Scan target_scan = (Scan) scan_ops.toArray()[scan_ops.size() - 1];
        assert (target_scan != null);
        Term scan_term = target_scan.getTerm();
        
        // Make sure the scan term has one absolute variable and get its target data source
        List<AbsoluteVariable> absolute_vars = LogicalPlanUtil.getAbsoluteVariables(scan_term.getVariablesUsed());
        assert (absolute_vars.size() == 1);
        String target_data_source = absolute_vars.get(0).getDataSourceName();
        
        // Create Insert operator
        Insert insert_op = new Insert(target_data_source, target_term);
        
        // Remove the scan at the top an plug the child below the Insert
        Operator child = ((Scan) op).getChild();
        op.removeChild(child);
        op = child;
        insert_op.addChild(op);
        op = insert_op;
        
        // Get the target collection type
        Type target_type = target_term.getType();
        
        // Unexpected target type
        if (!(target_type instanceof CollectionType))
        {
            throw new QueryCompilationException(QueryCompilation.DML_NON_COLLECTION_TARGET_TYPE, target_term.getLocation(),
                                                target_term.toString());
        }
        
        CollectionType collection_type = (CollectionType) target_type;
        
        // Unexpected target type
        if (!(collection_type.getChildrenType() instanceof TupleType))
        {
            throw new QueryCompilationException(QueryCompilation.DML_NON_COLLECTION_TARGET_TYPE, target_term.getLocation(),
                                                target_term.toString());
        }
        
        ValueExpression value_expr = insert_stmt.getValueExpression();
        
        // Set the target attributes
        List<String> target_attrs = new ArrayList<String>(insert_stmt.getTargetAttributes());
        if (target_attrs.isEmpty())
        {
            // Set the target attributes to be all the attributes in the collection type
            for (String attr_name : ((TupleType) collection_type.getChildrenType()).getAttributeNames())
                target_attrs.add(attr_name);
        }
        insert_op.setTargetAttributes(target_attrs);
        
        // If no query is specified, that is, it is either VALUES or DEFAULT VALUES
        if (value_expr == null)
        {
            // Construct the query node
            value_expr = new QuerySpecification(insert_stmt.getLocation());
            
            for (int i = 0; i < target_attrs.size(); i++)
            {
                // Get the target attribute type.
                Type target_attr_type = ((TupleType) collection_type.getChildrenType()).getAttribute(target_attrs.get(i));
                
                if (target_attr_type == null)
                {
                    // Romain: I don't see how this could happen. Check that this line is useful.
                    throw new QueryCompilationException(QueryCompilation.UNKNOWN_ATTRIBUTE, insert_stmt.getLocation(),
                                                        target_attrs.get(i));
                }
                
                // Translate the corresponding value expression to a plan.
                ValueExpression default_expr = null;
                if (insert_stmt.isDefaultValues())
                {
                    // Get default value
                    default_expr = AstUtil.translateValue(target_attr_type.getDefaultValue());
                }
                else
                {
                    default_expr = insert_stmt.getValueConstructors().get(i);
                }
                
                // Construct SELECT item
                SelectExpressionItem item = new SelectExpressionItem((ValueExpression) default_expr.copy(),
                                                                     default_expr.getLocation());
                item.setAlias(target_attrs.get(i));
                
                ((QuerySpecification) value_expr).addSelectItem(item);
            }
        }
        
        // Translate the query node to logical plan
        LogicalPlan insert_plan = build(value_expr, m_contexts, m_uas);
        insert_op.setInsertPlan(insert_plan);
        insert_op.updateOutputInfo();
        
        return op;
    }
    
    /**
     * Translates an UPDATE statement into a tree of operators.
     * 
     * @param update_stmt
     *            the statement to translate
     * @param root_op
     *            the root of the current tree of operators
     * @return the root of the corresponding tree of operators
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Operator translateUpdateStatement(UpdateStatement update_stmt, Operator root_op) throws QueryCompilationException
    {
        Operator op = translateAliasedDmlStatement(update_stmt, root_op);
        
        // Add a project at the top temporarily to translate the arguments as if they were in a SELECT clause.
        Project project = new Project();
        project.addChild(op);
        
        // Build assignments first, and then find the target data source
        Map<Term, LogicalPlan> assignments = new LinkedHashMap<Term, LogicalPlan>();
        for (Pair<AttributeReference, ValueExpression> pair : update_stmt.getUpdateList())
        {
            // Translate the attribute reference to the target. If the target of the statement is null, (UPDATE ... SET ...), we
            // should look in the scope of the query. Otherwise (SET object = ...), we should look in the entire scope.
            Term target = translateAttributeReference(pair.getKey(), project, (update_stmt.getTarget() != null));
            
            // Translate update source
            LogicalPlan assignment_plan = LogicalPlanBuilder.build(pair.getValue(), m_contexts, m_uas);
            
            assignments.put(target, assignment_plan);
        }
        
        // Get the target data source and the target query path
        String target_data_source = null;
        Term target_term = null;
        Scan target_scan = null;
        RelativeVariable update_var = null;
        if (update_stmt.getTarget() == null)
        {
            // Get the target data source and the target query path from the target of the single SET
            assert (assignments.size() == 1);
            target_term = assignments.keySet().iterator().next();
            
            // Make sure the scan term has one absolute variable and get its target data source
            if (target_term instanceof AbsoluteVariable)
            {
                target_data_source = ((AbsoluteVariable) target_term).getDataSourceName();
            }
            else if (target_term instanceof RelativeVariable)
            {
                assert (project.getChild() instanceof Navigate);
                Navigate nav = (Navigate) project.getChild();
                assert (nav.getAliasVariable().equals(target_term));
                List<AbsoluteVariable> absolute_vars = LogicalPlanUtil.getAbsoluteVariables(nav.getTerm().getVariablesUsed());
                assert (absolute_vars.size() == 1);
                target_data_source = absolute_vars.get(0).getDataSourceName();
            }
            else
            {
                throw new AssertionError();
            }
            
            try
            {
                DataSourceMetaData metadata = QueryProcessorFactory.getInstance().getDataSourceMetaData(target_data_source);
                
                if (metadata.getStorageSystem() == StorageSystem.JDBC)
                {
                    CheckedException ex = new DataSourceException(ExceptionMessages.DataSource.INVALID_STORAGE_SYSTEM,
                                                                  metadata.getName(), StorageSystem.JDBC.name());
                    throw new QueryCompilationException(QueryCompilation.INVALID_ACCESS, update_stmt.getLocation(), ex);
                }
            }
            catch (DataSourceException e)
            {
                throw new QueryCompilationException(QueryCompilation.INVALID_ACCESS, update_stmt.getLocation(), e);
            }
        }
        else
        {
            // Get the target data source from the scan operator
            Collection<? extends Scan> scan_ops = op.getDescendantsAndSelf(Scan.class);
            target_scan = (Scan) scan_ops.toArray()[scan_ops.size() - 1];
            assert (target_scan != null);
            Term scan_term = target_scan.getTerm();
            
            // Make sure the scan term has one absolute variable and get its target data source
            List<AbsoluteVariable> absolute_vars = LogicalPlanUtil.getAbsoluteVariables(scan_term.getVariablesUsed());
            assert (absolute_vars.size() == 1);
            target_data_source = absolute_vars.get(0).getDataSourceName();
            update_var = new RelativeVariable(target_scan.getAlias());
            
            // Get the relative variable corresponding to the target attribute reference.
            target_term = scan_term;
        }
        
        Update update_op = new Update(target_data_source, target_term, update_var, target_scan);
        Operator project_child = project.getChild();
        project.removeChild(project_child);
        update_op.addChild(project_child);
        
        // Add assignments
        for (Map.Entry<Term, LogicalPlan> entry : assignments.entrySet())
        {
            Assignment assignment = new Update.Assignment(entry.getKey(), entry.getValue());
            update_op.addAssignment(assignment);
        }
        
        update_op.updateOutputInfo();
        op = update_op;
        
        return op;
    }
    
    /**
     * Translate a WITH item.
     * 
     * @param ast_node
     *            the node to translate
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private void translateWithItem(WithItem ast_node) throws QueryCompilationException
    {
        String query_name = ast_node.getQueryName();
        if (getAssignByTarget(query_name) != null)
        {
            throw new QueryCompilationException(QueryCompilation.DUPLICATE_WITH_QUERY_NAME, ast_node.getLocation(), query_name);
        }
        
        // Parse the nested query
        LogicalPlan nested_plan = LogicalPlanBuilder.build(ast_node.getQueryExpression(), m_contexts, m_uas);
        Assign assign = new Assign(nested_plan, query_name);
        assign.updateOutputInfo();
        m_contexts.peek().addAssignTarget(assign.getTarget(), assign);
        m_assign_list.add(assign);
        
        // Create the temp schema object in the temp assign data source.
        try
        {
            LogicalPlanUtil.updateAssignedTempSchemaObject(assign);
        }
        catch (QueryExecutionException e)
        {
            // Chain the exception
            throw new QueryCompilationException(QueryCompilation.INVALID_ACCESS, ast_node.getLocation(), e);
        }
    }
    
    /**
     * Translate an ORDER BY item.
     * 
     * @param order_by_item
     *            the item to translate
     * @param orderby_op
     *            the Sort operator currently translated
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private void translateOrderByItem(OrderByItem order_by_item, Sort orderby_op) throws QueryCompilationException
    {
        Term sort_term = null;
        
        Nulls nulls = order_by_item.getNulls();
        Spec spec = order_by_item.getSpec();
        
        ValueExpression order_by_expr = order_by_item.getExpression();
        
        // Figure out if the item refers to an input or output attribute (cannot be a mix). If the order by item refers to an output
        // attribute, then the order by item has to be an AttributeReference of length 1 (but not vice versa).
        if (order_by_expr instanceof AttributeReference && ((AttributeReference) order_by_expr).getLength() == 1)
        {
            AttributeReference attr_ref = (AttributeReference) order_by_expr;
            // Check if the order by item refers to an output variable of the project operator and get the provenance term
            Operator parent = orderby_op.getParent();
            if (parent instanceof Project)
            {
                Project project = (Project) parent;
                for (Project.Item project_item : project.getProjectionItems())
                {
                    if (project_item.getAlias().equals(attr_ref.getPathSteps().get(0)))
                    {
                        // OrderBy item refers to output attribute.
                        if (attr_ref.getPathSteps().size() == 1)
                        {
                            sort_term = project_item.getTerm();
                        }
                        else
                        {
                            List<String> extra_steps = attr_ref.getPathSteps().subList(1, attr_ref.getPathSteps().size());
                            sort_term = new QueryPath(project_item.getTerm(), extra_steps);
                        }
                        break;
                    }
                }
            }
        }
        
        // OrderBy item refers to input attribute
        if (sort_term == null)
        {
            sort_term = translateValueExpression(order_by_expr, orderby_op);
        }
        
        orderby_op.addSortItem(sort_term, spec, nulls);
    }
    
    /**
     * Translate an offset expression.
     * 
     * @param offset_expr
     *            the expression to translate
     * @param offset_fetch_op
     *            the operator currently translated
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private void translateOffset(ValueExpression offset_expr, OffsetFetch offset_fetch_op) throws QueryCompilationException
    {
        Term offset_term = translateValueExpression(offset_expr, offset_fetch_op);
        offset_fetch_op.setOffset(offset_term);
    }
    
    /**
     * Translate an fetch expression.
     * 
     * @param fetch_expr
     *            the expression to translate
     * @param offset_fetch_op
     *            the operator currently translated
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private void translateFetch(ValueExpression fetch_expr, OffsetFetch offset_fetch_op) throws QueryCompilationException
    {
        Term fetch_term = translateValueExpression(fetch_expr, offset_fetch_op);
        offset_fetch_op.setFetch(fetch_term);
    }
    
    /**
     * Translate a FROM item.
     * 
     * @param from_item
     *            the AST node to translate
     * @return the root of the corresponding logical plan
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Operator translateFromItem(FromItem from_item) throws QueryCompilationException
    {
        // Normal form assumption: a from item is either a function call, a subquery, a join or an attribute reference
        // TODO implement FLATTEN feature
        if (from_item instanceof FromExpressionItem)
        {
            FromExpressionItem from_expr_item = (FromExpressionItem) from_item;
            ValueExpression from_expr = from_expr_item.getFromExpression();
            String alias = from_expr_item.getAlias();
            
            // Case 1: attribute reference
            if (from_expr instanceof AttributeReference)
            {
                AttributeReference attr_ref = (AttributeReference) from_expr;
                Ground ground = new Ground();
                ground.updateOutputInfo();
                Scan scan = new Scan(alias);
                scan.addChild(ground);
                
                // Translate the attribute reference.
                Term attr_term = translateValueExpression(attr_ref, scan);
                Type attr_term_type = attr_term.getType();
                assert (attr_term_type instanceof CollectionType);
                
                // If the alias was missing, use the default alias of the term.
                if (alias == null)
                {
                    alias = attr_term.getDefaultProjectAlias();
                    Scan new_scan = new Scan(alias);
                    Operator scan_child = scan.getChild();
                    scan.removeChild(scan_child);
                    new_scan.addChild(scan_child);
                    scan = new_scan;
                }
                
                // Set the scan term and the location of the variable
                scan.setTerm(attr_term);
                ElementVariable elem_var = scan.getAliasVariable();
                elem_var.setLocation(attr_ref.getLocation());
                
                // Handle the optional position variable
                String input_order_var = from_expr_item.getInputOrderVariable();
                if (input_order_var != null)
                {
                    PositionVariable position_rel_var = new PositionVariable(input_order_var);
                    position_rel_var.setLocation(from_expr.getLocation());
                    position_rel_var.setDefaultProjectAlias(input_order_var);
                    scan.setOrderVariable(position_rel_var);
                }
                
                scan.updateOutputInfo();
                
                return scan;
            }
            
            // Case 2: Subquery
            if (from_expr instanceof QueryNode)
            {
                assert (alias != null); // Ensured by the parser
                Subquery subquery = new Subquery(alias);
                Operator subquery_root = translateQueryNode((QueryNode) from_expr);
                subquery.addChild(subquery_root);
                
                // clear the scope from the variables created by the subquery, as they will not be directly accessible above (need
                // to go through the element variable)
                m_contexts.peek().clear();
                m_contexts.peek().setAboveGroupBy(false);
                
                assert (subquery_root.getOutputInfo().getVariables().size() == 1);
                RelativeVariable subquery_var = (RelativeVariable) subquery_root.getOutputInfo().getVariable(0);
                Type subquery_var_type = subquery_var.getType();
                
                ElementVariable elem_var = subquery.getAliasVariable();
                elem_var.setType(subquery_var_type);
                elem_var.setLocation(from_expr.getLocation());
                
                // Handle the optional position variable
                String input_order_var = from_expr_item.getInputOrderVariable();
                if (input_order_var != null)
                {
                    PositionVariable position_rel_var = new PositionVariable(input_order_var);
                    position_rel_var.setLocation(from_expr.getLocation());
                    position_rel_var.setDefaultProjectAlias(input_order_var);
                    subquery.setOrderVariable(position_rel_var);
                }
                
                subquery.updateOutputInfo();
                
                return subquery;
            }
            
            // Case 3: Function Node
            if (from_expr instanceof FunctionNode)
            {
                assert (alias != null); // Ensured by the parser
                FunctionNode func_node = (FunctionNode) from_expr;
                Ground ground = new Ground();
                ground.updateOutputInfo();
                Scan scan = new Scan(alias);
                scan.addChild(ground);
                
                // Translate the function node.
                Term attr_term = translateValueExpression(func_node, scan);
                Type attr_term_type = attr_term.getType();
                assert (attr_term_type instanceof CollectionType);
                
                // Set the scan term and the location of the variable
                scan.setTerm(attr_term);
                ElementVariable elem_var = scan.getAliasVariable();
                elem_var.setLocation(func_node.getLocation());
                
                // Handle the optional position variable
                String input_order_var = from_expr_item.getInputOrderVariable();
                if (input_order_var != null)
                {
                    PositionVariable position_rel_var = new PositionVariable(input_order_var);
                    position_rel_var.setLocation(from_expr.getLocation());
                    position_rel_var.setDefaultProjectAlias(input_order_var);
                    scan.setOrderVariable(position_rel_var);
                }
                
                scan.updateOutputInfo();
                
                return scan;
            }
            
            // At this point, we are trying to translate an unsupported FromExpressionItem
            throw new QueryCompilationException(QueryCompilation.INVALID_FROM_ITEM, from_item.getLocation(), from_item);
        }
        
        // Case 4: Join
        else if (from_item instanceof JoinItem)
        {
            JoinItem join_item = (JoinItem) from_item;
            Operator join_op = null;
            
            switch (join_item.getJoinType())
            {
                case INNER:
                    join_op = new InnerJoin();
                    break;
                case CROSS:
                    // The normal form specifies that there should be only one cross-product with join children.
                    join_op = new InnerJoin();
                    break;
                case LEFT_OUTER:
                    join_op = new OuterJoin(Variation.LEFT);
                    break;
                case RIGHT_OUTER:
                    join_op = new OuterJoin(Variation.RIGHT);
                    break;
                case FULL_OUTER:
                    join_op = new OuterJoin(Variation.FULL);
                    break;
            }
            
            // Translate children
            Operator left_child = translateFromItem(join_item.getLeftItem());
            Operator right_child = translateFromItem(join_item.getRightItem());
            join_op.addChild(left_child);
            join_op.addChild(right_child);
            
            // Put the variables in the scope before translating the join condition
            Set<String> variables = new HashSet<String>();
            for (Operator child : Arrays.asList(left_child, right_child))
            {
                for (RelativeVariable rel_var : child.getOutputInfo().getVariables())
                {
                    if (!variables.add(rel_var.getName()))
                    {
                        throw new QueryCompilationException(QueryCompilation.DUPLICATE_FROM_ITEM_ALIAS, rel_var.getLocation(),
                                                            rel_var);
                    }
                    
                    AttributeReference attr_ref = new AttributeReference(Arrays.asList(rel_var.getName()), rel_var.getLocation());
                    if (rel_var instanceof ElementVariable)
                    {
                        // We need to copy the variable because its binding index could be different below and above the join.
                        ElementVariable copy = (ElementVariable) rel_var.copy();
                        copy.setLocation(rel_var.getLocation());
                        copy.setBindingIndex(child.getOutputInfo().getVariableIndex(rel_var));
                        copy.setType(rel_var.getType());
                        m_contexts.peek().addMappingForSuffixMatch(attr_ref, copy);
                    }
                    else if (rel_var instanceof PositionVariable)
                    {
                        // We need to copy the variable because its binding index could be different below and above the join.
                        PositionVariable copy = (PositionVariable) rel_var.copy();
                        copy.setLocation(rel_var.getLocation());
                        copy.setBindingIndex(child.getOutputInfo().getVariableIndex(rel_var));
                        copy.setType(rel_var.getType());
                        m_contexts.peek().addMappingForExactMatch(attr_ref, copy);
                    }
                }
            }
            
            // Translate the join condition
            if (join_op instanceof InnerJoin && join_item.getJoinType() == JoinType.CROSS)
            {
                Term condition = new Constant(new BooleanValue(true));
                ((InnerJoin) join_op).addCondition(condition);
            }
            else if (join_op instanceof ConditionOperator)
            {
                if (!join_item.isNatural())
                {
                    // Translate the join condition
                    Term condition_term = translateValueExpression(join_item.getOnCondition(), join_op);
                    ((ConditionOperator) join_op).addCondition(condition_term);
                }
                else
                {
                    // TODO Support NATURAL JOIN
                    throw new UnsupportedOperationException();
                }
            }
            
            // Clear the context information added to translate the join condition, to not confuse the translation of other from
            // items
            m_contexts.peek().clear();
            m_contexts.peek().setAboveGroupBy(false);
            
            // Update the output info again to set the type of the condition
            join_op.updateOutputInfo();
            
            return join_op;
        }
        else if (from_item instanceof FlattenItem)
        {
            FlattenItem flatten_item = (FlattenItem) from_item;
            
            // Translate children
            Operator left_child = translateFromItem(flatten_item.getLeftItem());
            
            FromExpressionItem right = flatten_item.getRightItem();
            String right_alias = right.getAlias();
            ValueExpression right_val_expr = right.getFromExpression();
            assert (right_val_expr instanceof AttributeReference);
            AttributeReference right_attr_ref = (AttributeReference) right_val_expr;
            
            // Put the variables of the left side in the scope before translating the right child
            Set<String> variables = new HashSet<String>();
            for (RelativeVariable rel_var : left_child.getOutputInfo().getVariables())
            {
                if (!variables.add(rel_var.getName()))
                {
                    throw new QueryCompilationException(QueryCompilation.DUPLICATE_FROM_ITEM_ALIAS, rel_var.getLocation(), rel_var);
                }
                
                AttributeReference attr_ref = new AttributeReference(Arrays.asList(rel_var.getName()), rel_var.getLocation());
                if (rel_var instanceof ElementVariable)
                {
                    m_contexts.peek().addMappingForSuffixMatch(attr_ref, rel_var);
                }
                else if (rel_var instanceof PositionVariable)
                {
                    m_contexts.peek().addMappingForExactMatch(attr_ref, rel_var);
                }
            }
            
            // Translate the attribute reference
            Scan flatten_scan = new Scan(right_alias);
            LogicalPlanUtil.insertOnTop(left_child, flatten_scan);
            m_attr_ref_to_navigate = false;
            Term term = translateAttributeReference(right_attr_ref, flatten_scan, true);
            m_attr_ref_to_navigate = true;
            
            assert (term instanceof QueryPath);
            flatten_scan.setTerm(term);
            
            switch (flatten_item.getFlattenType())
            {
                case INNER:
                    flatten_scan.setFlattenSemantics(FlattenSemantics.INNER);
                    break;
                case LEFT_OUTER:
                    flatten_scan.setFlattenSemantics(FlattenSemantics.OUTER);
                    break;
                default:
                    throw new AssertionError();
            }
            
            // Clear the context information added to translate the join condition, to not confuse the translation of other from
            // items
            m_contexts.peek().clear();
            m_contexts.peek().setAboveGroupBy(false);
            
            // Update the output infon
            flatten_scan.updateOutputInfo();
            
            return flatten_scan;
        }
        
        // At this point, we are trying to translate an unsupported FromItem
        throw new QueryCompilationException(QueryCompilation.INVALID_FROM_ITEM, from_item.getLocation(), from_item);
    }
    
    /**
     * Translate a WHERE item.
     * 
     * @param where_item
     *            the item to translate
     * @param select_op
     *            the Select operator currently translated
     * @return the translated term so that the caller can handle short-circuiting
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Term translateWhereItem(ValueExpression where_item, Select select_op) throws QueryCompilationException
    {
        // Translate the where item
        Term where_term = translateValueExpression(where_item, select_op);
        
        // Add the condition
        select_op.addCondition(where_term);
        
        // Return the translated term for short-circuiting handling by the caller
        return where_term;
    }
    
    /**
     * Translate a GROUP BY item.
     * 
     * @param group_by_item
     *            the item to translate
     * @param group_by_op
     *            the GroupBy operator currently translated
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private void translateGroupByItem(GroupByItem group_by_item, GroupBy group_by_op) throws QueryCompilationException
    {
        // Translate the group by item
        Term group_by_term = translateValueExpression(group_by_item.getExpression(), group_by_op);
        
        // Add the condition
        group_by_op.addGroupByTerm(group_by_term);
    }
    
    /**
     * Translate a HAVING item.
     * 
     * @param having_item
     *            the item to translate
     * @param select_op
     *            the Select operator currently translated
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private void translateHavingItem(ValueExpression having_item, Select select_op) throws QueryCompilationException
    {
        // Translate the having item
        Term having_term = translateValueExpression(having_item, select_op);
        
        // Add the condition
        select_op.addCondition(having_term);
    }
    
    /**
     * Translate a SELECT item.
     * 
     * @param select_item
     *            the item to translate
     * @param project_op
     *            the Project operator currently translated
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private void translateSelectItem(SelectItem select_item, Project project_op) throws QueryCompilationException
    {
        if (select_item instanceof SelectAllItem && select_item.getChildren().size() == 0)
        {
            // SELECT * FROM
            translateSelectAllItemUnqualifiedAsterisk((SelectAllItem) select_item, project_op);
        }
        else if (select_item instanceof SelectAllItem)
        {
            // SELECT t.* FROM
            translateSelectAllItemQualifiedAsterisk((SelectAllItem) select_item, project_op);
        }
        else if (select_item instanceof SelectExpressionItem)
        {
            // SELECT t FROM
            translateSelectExpressionItem((SelectExpressionItem) select_item, project_op);
        }
    }
    
    /**
     * Translate a SELECT *.
     * 
     * @param select_item
     *            the item to translate
     * @param project_op
     *            the Project operator currently translated
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private void translateSelectAllItemUnqualifiedAsterisk(SelectAllItem select_item, Project project_op)
            throws QueryCompilationException
    {
        // SELECT * is not compatible with GROUP BY clause
        if (m_contexts.peek().isAboveGroupBy())
        {
            throw new QueryCompilationException(QueryCompilation.SELECT_STAR_WITH_GROUP_BY, select_item.getLocation(), select_item);
        }
        
        // Iterate over the element variables in the current context and add them as projection items
        LogicalPlanBuilderContext current_context = m_contexts.peek();
        Collection<RelativeVariable> rel_vars = current_context.getVariablesForSuffixMatch();
        boolean at_least_one = false;
        for (RelativeVariable rel_var : rel_vars)
        {
            if (rel_var instanceof ElementVariable)
            {
                assert (rel_var.getType() instanceof TupleType);
                TupleType tuple_type = (TupleType) rel_var.getType();
                for (String name : tuple_type.getAttributeNames())
                {
                    String[] steps = { rel_var.getName(), name };
                    AttributeReference temp_attr_ref = new AttributeReference(Arrays.asList(steps), select_item.getLocation());
                    Term item_term = translateAttributeReference(temp_attr_ref, project_op, true);
                    item_term.setLocation(select_item.getLocation());
                    project_op.addProjectionItem(item_term, name, false);
                }
                at_least_one = true;
            }
        }
        assert (at_least_one);
    }
    
    /**
     * Translate a SELECT t.*.
     * 
     * @param select_item
     *            the item to translate
     * @param project_op
     *            the Project operator currently translated
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private void translateSelectAllItemQualifiedAsterisk(SelectAllItem select_item, Project project_op)
            throws QueryCompilationException
    {
        // SELECT * is not compatible with GROUP BY clause
        if (m_contexts.peek().isAboveGroupBy())
        {
            throw new QueryCompilationException(QueryCompilation.SELECT_STAR_WITH_GROUP_BY, select_item.getLocation(), select_item);
        }
        
        AttributeReference attr_ref = (AttributeReference) select_item.getChildren().get(0);
        
        // Find the corresponding query path in the contexts, current or outer ones, by iterating over the contexts in reverse
        // order.
        ListIterator<LogicalPlanBuilderContext> iter = m_contexts.listIterator(m_contexts.size());
        boolean success = false;
        while (iter.hasPrevious())
        {
            LogicalPlanBuilderContext current_context = iter.previous();
            
            // Try to resolve the path in this context
            Term matched_term = current_context.resolveSuffix(attr_ref);
            
            if (matched_term != null)
            {
                // Success
                success = true;
                
                List<String> steps = new ArrayList<String>();
                if (matched_term instanceof RelativeVariable)
                {
                    steps.add(((RelativeVariable) matched_term).getName());
                }
                else
                {
                    steps.add(((RelativeVariable) ((QueryPath) matched_term).getTerm()).getName());
                    steps.addAll(((QueryPath) matched_term).getPathSteps());
                }
                
                // Find the type of the matched term
                Type matched_term_type = matched_term.getType();
                assert (matched_term_type instanceof TupleType);
                TupleType tuple_type = (TupleType) matched_term_type;
                
                // Add all the attributes to the projection list
                for (String name : tuple_type.getAttributeNames())
                {
                    List<String> attr_steps = new ArrayList<String>(steps);
                    attr_steps.add(name);
                    AttributeReference temp_attr_ref = new AttributeReference(attr_steps, select_item.getLocation());
                    Term item_term = translateAttributeReference(temp_attr_ref, project_op, true);
                    item_term.setLocation(select_item.getLocation());
                    project_op.addProjectionItem(item_term, name, false);
                }
            }
            if (success) break;
        }
        if (!success)
        {
            throw new QueryCompilationException(QueryCompilation.UNKNOWN_ATTRIBUTE, attr_ref.getLocation(), attr_ref.toString());
        }
    }
    
    /**
     * Translate a SELECT expression item.
     * 
     * @param select_item
     *            the item to translate
     * @param project_op
     *            the Project operator currently translated
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private void translateSelectExpressionItem(SelectExpressionItem select_item, Project project_op)
            throws QueryCompilationException
    {
        ValueExpression expression = select_item.getExpression();
        if (expression instanceof SwitchNode)
        {
            List<Pair<Term, String>> attributes = translateSwitchNode((SwitchNode) expression, select_item.getAlias(), project_op);
            for (Pair<Term, String> attr : attributes)
            {
                project_op.addProjectionItem(attr.getKey(), attr.getValue(), false);
            }
        }
        else
        {
            Term item_term = translateValueExpression(expression, project_op);
            
            String alias = select_item.getAlias();
            
            if (alias == null)
            {
                // Use default alias
                alias = item_term.getDefaultProjectAlias();
                project_op.addProjectionItem(item_term, alias, false);
            }
            else
            {
                project_op.addProjectionItem(item_term, alias, true);
            }
        }
    }
    
    /**
     * Translates a value expression.
     * 
     * @param expr
     *            the expression to translate
     * @param op
     *            the operator currently translated
     * @return the translated term
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Term translateValueExpression(ValueExpression expr, Operator op) throws QueryCompilationException
    {
        // Dispatch the value expressions that cannot be grouping items
        if (expr instanceof QueryNode) return translateNestedQueryNode((QueryNode) expr, op);
        if (expr instanceof AggregateFunctionNode) return translateAggregateFunctionNode((AggregateFunctionNode) expr, op);
        if (expr instanceof ExistsNode) return translateExistsNode((ExistsNode) expr, op);
        if (expr instanceof TupleFunctionNode) return translateTupleFunctionNode((TupleFunctionNode) expr, op);
        if (expr instanceof CollectionFunctionNode) return translateCollectionFunctionNode((CollectionFunctionNode) expr, op);
        
        // Dispatch the value expressions for which the method implements a special treatment for grouping items
        if (expr instanceof AttributeReference) return translateAttributeReference((AttributeReference) expr, op, false);
        
        // Check if the value expression is a grouping item.
        if (m_contexts.peek().isAboveGroupBy() && m_contexts.peek().matchValueExpressionAboveGroupBy(expr) != null)
        {
            return m_contexts.peek().matchValueExpressionAboveGroupBy(expr);
        }
        
        // Dispatch the remaining value expressions
        if (expr instanceof BooleanLiteral) return translateBooleanLiteral((BooleanLiteral) expr, op);
        if (expr instanceof CaseFunctionNode) return translateCaseFunctionNode((CaseFunctionNode) expr, op);
        if (expr instanceof CastFunctionNode) return translateCastFunctionNode((CastFunctionNode) expr, op);
        if (expr instanceof ExternalFunctionNode) return translateExternalFunctionNode((ExternalFunctionNode) expr, op);
        if (expr instanceof GeneralFunctionNode) return translateGeneralFunctionNode((GeneralFunctionNode) expr, op);
        if (expr instanceof NullLiteral) return translateNullLiteral((NullLiteral) expr, op);
        if (expr instanceof NumericLiteral) return translateNumericLiteral((NumericLiteral) expr, op);
        if (expr instanceof StringLiteral) return translateStringLiteral((StringLiteral) expr, op);
        
        // The value expression is not supported
        throw new UnsupportedOperationException();
    }
    
    /**
     * Translates an attribute reference.
     * 
     * @param expr
     *            the attribute reference to translate
     * @param op
     *            the operator current translated
     * 
     * @param sql_scope_only
     *            tells if the variable should only be looked for in the scope of the query. This is a convenience access for the
     *            translation of SELECT *, so that they don't raise ambiguity errors. By default, it should be false.
     * @return the translated term
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Term translateAttributeReference(AttributeReference expr, Operator op, boolean sql_scope_only)
            throws QueryCompilationException
    {
        // This method will try to resolve the attribute reference in the following scopes:
        // - Absolute variables, e.g. data_source_name.table_name
        // - FPL local scope, i.e. variables coming from the UI markup language
        // - The list of contexts
        // If no match is found, an error is raised.
        // In the list of contexts, the inner context will be checked before outer contexts (variable shadowing). If many matches
        // are found in a same context, an error is raised.
        // If a match is found in two or more of these three scopes, an error is raised
        // Otherwise, the single match is returned.
        //
        // This method returns a term as follows:
        // 1- If expr is a path starting from a function call, it returns the corresponding query path
        // 2- If expr is the name of a variable in the current context, it returns the corresponding variable
        // 3- If expr is the name of a variable in an outer context, it returns the corresponding parameter
        // 4- If expr is the name of a variable in the FPL scope, it returns the corresponding variable
        // 5- If expr is the name of a datasource and a data object, it returns the corresponding absolute variable
        // 6- If expr falls in cases 2-5 but has extra path steps (navigations):
        // 6.1 If m_attr_ref_to_navigate is false or if op is a Scan, the returned value in cases 2-5 are wrapped into a query path
        // 6.2 Otherwise, a Navigate is placed under op and navigates from the returned value in cases 2-5 to the corresponding path
        
        // This map is used to detect ambiguous attribute references.
        Map<String, Term> matched_terms = new HashMap<String, Term>();
        String matched_terms_absolute_var = "Datasource names";
        String matched_terms_fpl = "FPL context";
        String matched_terms_context = "SQL++ query context";
        
        // Stores the matched context for further use in case the attribute references matches a variable in the current context or
        // an outer context.
        LogicalPlanBuilderContext matched_context = null;
        
        List<String> attr_ref_steps = new ArrayList<String>(expr.getPathSteps());
        
        // 1- If expr is a path starting from a function call, it returns the corresponding query path
        if (expr.getStartingFunctionNode() != null)
        {
            assert (attr_ref_steps.size() > 0); // Otherwise the input would be a FunctionNode instead of an attribute reference
            
            Term func_term = translateValueExpression(expr.getStartingFunctionNode(), op);
            
            Term term = new QueryPath(func_term, attr_ref_steps);
            term.setLocation(expr.getLocation());
            term.inferType(new ArrayList<Operator>(op.getDescendants()));
            
            return term;
        }
        
        // Check if the attribute reference starts with a data source name and a data object name
        if (!sql_scope_only && attr_ref_steps.size() >= 2 && m_uas.getDataSourceNames().contains(attr_ref_steps.get(0)))
        {
            DataSource ds = null;
            try
            {
                ds = m_uas.getDataSource(attr_ref_steps.get(0));
            }
            catch (DataSourceException e)
            {
                throw new AssertionError();
            }
            if (ds.hasSchemaObject(attr_ref_steps.get(1)))
            {
                // Success
                Term term = new AbsoluteVariable(attr_ref_steps.get(0), attr_ref_steps.get(1));
                term.setLocation(expr.getLocation());
                
                // If the attribute reference has extra path steps, wrap the term in a query path
                if (attr_ref_steps.size() > 2)
                {
                    term = new QueryPath(term, attr_ref_steps.subList(2, attr_ref_steps.size()));
                    term.setLocation(expr.getLocation());
                }
                
                // Put the match in the matched terms map
                matched_terms.put(matched_terms_absolute_var, term);
            }
        }
        
        // Check if the attribute reference refers to an attribute in the FPL local scope data source.
        if (!sql_scope_only && !m_uas.isFplStackEmpty())
        {
            List<String> ar_steps = new ArrayList<String>(attr_ref_steps);
            
            Term out_term = null;
            DataSource local_scope = null;
            
            try
            {
                local_scope = m_uas.getDataSource(DataSource.FPL_LOCAL_SCOPE);
            }
            catch (DataSourceException e)
            {
                // This should never happen
                throw new AssertionError(e);
            }
            
            // Handle the attribute reference to the context.
            if (ar_steps.get(0).equals(DataSource.CONTEXT) && local_scope.hasSchemaObject(DataSource.CONTEXT))
            {
                out_term = new AbsoluteVariable(DataSource.FPL_LOCAL_SCOPE, DataSource.CONTEXT);
                if (ar_steps.size() > 1)
                {
                    out_term = new QueryPath(out_term, ar_steps.subList(1, ar_steps.size()));
                }
                out_term.setLocation(expr.getLocation());
            }
            else
            {
                List<SchemaObject> schema_objs = new ArrayList<SchemaObject>(local_scope.getSchemaObjects());
                if (local_scope.hasSchemaObject(DataSource.CONTEXT))
                {
                    try
                    {
                        schema_objs.remove(local_scope.getSchemaObject(DataSource.CONTEXT));
                    }
                    catch (QueryExecutionException e)
                    {
                        throw new AssertionError(e);
                    }
                }
                if (schema_objs.size() == 1)
                {
                    SchemaObject schema_obj = schema_objs.get(0);
                    TupleType local = (TupleType) schema_obj.getSchemaTree().getRootType();
                    if (local.hasAttribute(ar_steps.get(0)))
                    {
                        out_term = new AbsoluteVariable(DataSource.FPL_LOCAL_SCOPE, schema_obj.getName());
                        out_term.setLocation(expr.getLocation());
                        out_term = new QueryPath(out_term, ar_steps);
                        out_term.setLocation(expr.getLocation());
                    }
                }
            }
            
            if (out_term != null)
            {
                matched_terms.put(matched_terms_fpl, out_term);
            }
        }
        
        // Find the corresponding query path in the contexts, current one first, then outer ones.
        ListIterator<LogicalPlanBuilderContext> iter = m_contexts.listIterator(m_contexts.size());
        while (iter.hasPrevious())
        {
            LogicalPlanBuilderContext context = iter.previous();
            Term term = null;
            if (context.isAboveGroupBy())
            {
                term = context.matchValueExpressionAboveGroupBy(expr);
            }
            if (term == null && (!context.isAboveGroupBy() || op instanceof GroupBy))
            {
                term = context.resolveSuffix(expr);
            }
            if (term != null)
            {
                // If the match is in an outer context, wrap the term in a Parameter.
                if (context != m_contexts.peek())
                {
                    assert (term instanceof Variable || term instanceof QueryPath);
                    if (term instanceof RelativeVariable)
                    {
                        term = new Parameter(term);
                        term.inferType(Collections.<Operator> emptyList());
                    }
                    else if (term instanceof QueryPath && ((QueryPath) term).getTerm() instanceof RelativeVariable)
                    {
                        QueryPath term_qp = (QueryPath) term;
                        term = new QueryPath(new Parameter(term_qp.getTerm()), term_qp.getPathSteps());
                        term.inferType(Collections.<Operator> emptyList());
                    }
                    else
                    {
                        // Keep the term as is
                        assert (term instanceof AbsoluteVariable || ((QueryPath) term).getTerm() instanceof AbsoluteVariable);
                    }
                }
                // Set the new location
                term.setLocation(expr.getLocation());
                matched_terms.put(matched_terms_context, term);
                matched_context = context;
                break;
            }
        }
        
        // Raise an error if no match was found
        if (matched_terms.keySet().size() == 0)
        {
            throw new QueryCompilationException(QueryCompilation.UNKNOWN_ATTRIBUTE, expr.getLocation(), expr.toString());
        }
        
        // Raise an error if multiple matches were found
        if (matched_terms.keySet().size() > 1)
        {
            String msg_details = "Matches were found in the following scopes: ";
            for (String scope : matched_terms.keySet())
            {
                msg_details += scope;
                msg_details += ", ";
            }
            msg_details = msg_details.substring(0, msg_details.length() - 2);
            msg_details += ".";
            throw new QueryCompilationException(QueryCompilation.AMBIGUOUS_QUERY_PATH, expr.getLocation(), expr.toString(),
                                                msg_details);
        }
        
        // Otherwise, get the single match and the type of match
        String matched_term_scope = matched_terms.keySet().iterator().next();
        Term matched_term = matched_terms.get(matched_term_scope);
        
        // If expr starts with the name of a variable in the current context
        if (matched_term_scope.equals(matched_terms_context) && matched_context == m_contexts.peek())
        {
            // matched_term is either a relative variable or a query path starting with a relative variable
            assert (matched_term instanceof Variable || matched_term instanceof QueryPath);
            
            if (matched_term instanceof Variable)
            {
                matched_term.inferType(new ArrayList<Operator>(op.getDescendants()));
                return matched_term;
            }
            else if (!m_attr_ref_to_navigate || op instanceof Scan)
            {
                assert (((QueryPath) matched_term).getTerm() instanceof Variable);
                matched_term.inferType(new ArrayList<Operator>(op.getDescendants()));
                return matched_term;
            }
            else
            {
                assert (((QueryPath) matched_term).getTerm() instanceof Variable);
                Variable matched_term_fst_step = (Variable) ((QueryPath) matched_term).getTerm();
                
                String alias = NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.VARIABLE_GENERATOR);
                Navigate nav = new Navigate(alias, matched_term);
                if (op instanceof UnaryOperator)
                {
                    LogicalPlanUtil.insertOnTop(((UnaryOperator) op).getChild(), nav);
                }
                else
                {
                    Operator target_child = null;
                    for (Operator child : op.getChildren())
                    {
                        if (child.getOutputInfo().getVariables().contains(matched_term_fst_step))
                        {
                            target_child = child;
                        }
                    }
                    assert (target_child != null);
                    LogicalPlanUtil.insertOnTop(target_child, nav);
                }
                nav.updateOutputInfo();
                return nav.getAliasVariable();
            }
        }
        
        // If expr is the name of a variable in an outer context
        if (matched_term_scope.equals(matched_terms_context) && matched_context != m_contexts.peek())
        {
            // matched_term is either a parameter or a query path starting with a parameter
            assert (matched_term instanceof Parameter || matched_term instanceof AbsoluteVariable || matched_term instanceof QueryPath);
            
            if (matched_term instanceof Parameter || matched_term instanceof AbsoluteVariable)
            {
                matched_term.inferType(new ArrayList<Operator>(op.getDescendants()));
                return matched_term;
            }
            else if (matched_term instanceof QueryPath && ((QueryPath) matched_term).getTerm() instanceof AbsoluteVariable)
            {
                matched_term.inferType(new ArrayList<Operator>(op.getDescendants()));
                return matched_term;
            }
            else if (!m_attr_ref_to_navigate || op instanceof Scan)
            {
                assert (((QueryPath) matched_term).getTerm() instanceof Parameter);
                matched_term.inferType(new ArrayList<Operator>(op.getDescendants()));
                return matched_term;
            }
            else
            {
                assert (((QueryPath) matched_term).getTerm() instanceof Parameter);
                
                String alias = NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.VARIABLE_GENERATOR);
                Navigate nav = new Navigate(alias, matched_term);
                if (op instanceof UnaryOperator)
                {
                    LogicalPlanUtil.insertOnTop(((UnaryOperator) op).getChild(), nav);
                    nav.updateOutputInfo();
                    return nav.getAliasVariable();
                }
                else
                {
                    // Join condition using a parameter. There is no point putting a Navigate operator in this case.
                    matched_term.inferType(new ArrayList<Operator>(op.getDescendants()));
                    return matched_term;
                }
            }
        }
        
        // If expr is the name of a variable in the FPL scope or the name of a datasource and a data object
        if (matched_term_scope.equals(matched_terms_fpl) || matched_term_scope.equals(matched_terms_absolute_var))
        {
            // matched_term is either an absolute variable or a query path starting with an absolute variable
            assert (matched_term instanceof AbsoluteVariable || matched_term instanceof QueryPath);
            
            if (matched_term instanceof AbsoluteVariable)
            {
                matched_term.inferType(new ArrayList<Operator>(op.getDescendants()));
                return matched_term;
            }
            else if (!m_attr_ref_to_navigate || op instanceof Scan)
            {
                assert (((QueryPath) matched_term).getTerm() instanceof AbsoluteVariable);
                matched_term.inferType(new ArrayList<Operator>(op.getDescendants()));
                return matched_term;
            }
            else
            {
                assert (((QueryPath) matched_term).getTerm() instanceof AbsoluteVariable);
                
                String alias = NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.VARIABLE_GENERATOR);
                Navigate nav = new Navigate(alias, matched_term);
                if (op instanceof UnaryOperator)
                {
                    LogicalPlanUtil.insertOnTop(((UnaryOperator) op).getChild(), nav);
                    nav.updateOutputInfo();
                    return nav.getAliasVariable();
                }
                else
                {
                    // Join condition using an absolute variable. There is no point putting a Navigate operator in this case.
                    matched_term.inferType(new ArrayList<Operator>(op.getDescendants()));
                    return matched_term;
                }
            }
        }
        
        // Previous if-blocks handled all the cases for matched_term_scope
        throw new AssertionError();
    }
    
    /**
     * Translates a nested query.
     * 
     * @param expr
     *            the query node to translate
     * @param op
     *            the operator current translated
     * @return the translated term
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Term translateNestedQueryNode(QueryNode expr, Operator op) throws QueryCompilationException
    {
        // Nested queries are not supported in the LIMIT clause
        if (op instanceof OffsetFetch)
        {
            throw new QueryCompilationException(QueryCompilation.NESTED_QUERY_IN_OFFSET_FETCH, expr.getLocation());
        }
        
        // Nested queries are not supported in the ORDER BY clause if there is a set operator.
        if (op instanceof Sort && LogicalPlanUtil.getNextClauseOperator(op) instanceof SetOperator)
        {
            throw new QueryCompilationException(QueryCompilation.NESTED_QUERY_IN_ORDER_BY_WITH_SET_OP, expr.getLocation());
        }
        
        // Nested queries are not supported in JOIN conditions
        if (!(op instanceof UnaryOperator))
        {
            throw new QueryCompilationException(ExceptionMessages.QueryCompilation.INVALID_NESTED_QUERY_LOCATION,
                                                expr.getLocation());
        }
        
        // Parse the nested query
        LogicalPlan nested_plan = LogicalPlanBuilder.build(expr, m_contexts, m_uas);
        
        String fresh_alias = NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.VARIABLE_GENERATOR);
        ApplyPlan apply_plan = new ApplyPlan(fresh_alias, nested_plan);
        
        // Set the execution condition to the current branch execution condition
        if (!m_branch_conditions.isEmpty())
        {
            apply_plan.setExecutionCondition(getExecutionConditionConjunct());
        }
        
        // Add the ApplyPlan below op
        LogicalPlanUtil.insertOnTop(((UnaryOperator) op).getChild(), apply_plan);
        
        apply_plan.updateOutputInfo();
        
        RelativeVariable apply_var = apply_plan.getAliasVariable();
        apply_var.setType(nested_plan.getOutputType());
        apply_var.setLocation(expr.getLocation());
        
        return apply_var;
    }
    
    /**
     * Translates an aggregate function node.
     * 
     * @param expr
     *            the aggregate function to translate
     * @param op
     *            the operator current translated
     * @return the translated term
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Term translateAggregateFunctionNode(AggregateFunctionNode expr, Operator op) throws QueryCompilationException
    {
        // Aggregate functions are only allowed in SELECT and HAVING clauses
        assert (op instanceof Project || op instanceof Select);
        
        // Find the GroupBy operator using normal form assumptions
        Operator operator = op;
        GroupBy groupby_op = null;
        if (operator instanceof Project) operator = LogicalPlanUtil.getNextClauseOperator(operator);
        if (operator instanceof OffsetFetch) operator = LogicalPlanUtil.getNextClauseOperator(operator);
        if (operator instanceof Sort) operator = LogicalPlanUtil.getNextClauseOperator(operator);
        if (operator instanceof Select)
        {
            if (LogicalPlanUtil.getNextClauseOperator(operator) instanceof GroupBy)
            {
                // This Select is the HAVING operator
                groupby_op = (GroupBy) LogicalPlanUtil.getNextClauseOperator(operator);
            }
            else
            {
                // This Select is the WHERE operator and there was no GroupBy. This means the aggregation function corresponds to
                // total aggregation (aggregation without GROUP BY clause). We create a GroupBy operator there.
                groupby_op = new GroupBy();
                LogicalPlanUtil.insertOnTop(operator, groupby_op);
            }
        }
        else if (operator instanceof GroupBy)
        {
            groupby_op = (GroupBy) operator;
        }
        else
        {
            // There was no GroupBy. This means the aggregation function corresponds to total aggregation (aggregation without GROUP
            // BY clause). We create a GroupBy operator there.
            groupby_op = new GroupBy();
            LogicalPlanUtil.insertOnTop(operator, groupby_op);
        }
        
        String agg_function = expr.getFunctionName();
        SetQuantifier agg_quantifier = expr.getSetQuantifier();
        List<ValueExpression> arguments = expr.getArguments();
        
        List<Term> term_arguments = new ArrayList<Term>();
        for (ValueExpression argument : arguments)
        {
            // The function call we be added to the GroupBy operator, therefore Navigate, Exists and ApplyPlan should be added below
            // the GroupBy
            term_arguments.add(translateValueExpression(argument, groupby_op));
        }
        
        Function func = null;
        try
        {
            func = FunctionRegistry.getInstance().getFunction(agg_function);
        }
        catch (FunctionRegistryException e)
        {
            // This should never happen because the parser only accepts valid aggregate functions
            throw new AssertionError(e);
        }
        
        AggregateFunctionCall agg_call = new AggregateFunctionCall((AggregateFunction) func, agg_quantifier, term_arguments);
        agg_call.setLocation(expr.getLocation());
        
        // Fresh alias for the aggregate function call
        String fresh_alias = agg_call.getDefaultProjectAlias()
                + NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.VARIABLE_GENERATOR);
        
        // Add aggregate to group by operator
        groupby_op.addAggregate(agg_call, fresh_alias);
        
        LogicalPlanUtil.updateAncestorOutputInfo(groupby_op);
        
        RelativeVariable result_var = new RelativeVariable(fresh_alias);
        agg_call.inferType(Collections.<Operator> singletonList(groupby_op.getChild()));
        result_var.setType(agg_call.getType());
        
        return result_var;
    }
    
    /**
     * Translates a boolean literal.
     * 
     * @param expr
     *            the boolean literal to translate
     * @param op
     *            the operator current translated
     * @return the translated term
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Term translateBooleanLiteral(BooleanLiteral expr, Operator op) throws QueryCompilationException
    {
        Constant constant = new Constant(expr.getScalarValue());
        constant.setLocation(expr.getLocation());
        constant.inferType(Collections.<Operator> emptyList());
        
        return constant;
    }
    
    /**
     * Translates a CASE WHEN function node.
     * 
     * @param expr
     *            the function node to translate
     * @param op
     *            the operator current translated
     * @return the translated term
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Term translateCaseFunctionNode(CaseFunctionNode expr, Operator op) throws QueryCompilationException
    {
        List<Term> args = new ArrayList<Term>();
        Iterator<ValueExpression> iter_args = expr.getArguments().iterator();
        
        while (iter_args.hasNext())
        {
            Term when = translateValueExpression(iter_args.next(), op);
            args.add(when);
            // The "then" branch is only executed if the "when" condition is true
            m_branch_conditions.push(when);
            Term then = translateValueExpression(iter_args.next(), op);
            args.add(then);
            // The next "when" branches are only executed if this "when" branch is false or null
            m_branch_conditions.pop();
            Term null_when = new GeneralFunctionCall(new IsNullFunction(), when);
            Term not_when = new GeneralFunctionCall(new NotFunction(), when);
            Term or_when = new GeneralFunctionCall(new OrFunction(), null_when, not_when);
            m_branch_conditions.push(or_when);
        }
        
        // Remove all the conditions introduced by this Case/When from the stack
        for (int i = 0; i < expr.getArguments().size() / 2; i++)
        {
            m_branch_conditions.pop();
        }
        
        CaseFunctionCall call = new CaseFunctionCall(args);
        call.setLocation(expr.getLocation());
        call.inferType(op.getChildren());
        
        return call;
    }
    
    /**
     * Translates a CAST function node.
     * 
     * @param expr
     *            the function_node to translate
     * @param op
     *            the operator current translated
     * @return the translated term
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Term translateCastFunctionNode(CastFunctionNode expr, Operator op) throws QueryCompilationException
    {
        Term term = translateValueExpression(expr.getArgument(), op);
        term.setLocation(expr.getArgument().getLocation());
        
        TypeEnum target_type = null;
        try
        {
            target_type = TypeEnum.getEntry(expr.getTargetTypeName());
        }
        catch (TypeException e)
        {
            // This should never happen because the parser only accepts valid type names
            throw new AssertionError(e);
        }
        
        CastFunctionCall call = new CastFunctionCall(term, target_type);
        call.setLocation(expr.getLocation());
        call.inferType(op.getChildren());
        
        return call;
    }
    
    /**
     * Translates an EXISTS node.
     * 
     * @param expr
     *            the node to translate
     * @param op
     *            the operator current translated
     * @return the translated term
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Term translateExistsNode(ExistsNode expr, Operator op) throws QueryCompilationException
    {
        String fresh_alias = NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.VARIABLE_GENERATOR);
        LogicalPlan exists_plan = build(expr.getValueExpression(), m_contexts, m_uas);
        Exists exists_op = new Exists(fresh_alias, exists_plan);
        
        // Set the execution condition to the current branch execution condition
        if (!m_branch_conditions.isEmpty())
        {
            exists_op.setExecutionCondition(getExecutionConditionConjunct());
        }
        
        if (op instanceof UnaryOperator)
        {
            LogicalPlanUtil.insertOnTop(((UnaryOperator) op).getChild(), exists_op);
        }
        else
        {
            throw new QueryCompilationException(ExceptionMessages.QueryCompilation.INVALID_EXISTS_LOCATION, expr.getLocation());
        }
        exists_op.updateOutputInfo();
        
        // Construct the condition
        RelativeVariable condition = new RelativeVariable(fresh_alias);
        condition.setType(TypeEnum.BOOLEAN.get());
        condition.setLocation(expr.getLocation());
        
        return condition;
    }
    
    /**
     * Translates an external function node.
     * 
     * @param expr
     *            the node to translate
     * @param op
     *            the operator current translated
     * @return the translated term
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Term translateExternalFunctionNode(ExternalFunctionNode expr, Operator op) throws QueryCompilationException
    {
        String target_data_source_name = expr.getTargetDataSource();
        
        // Make sure the function exists
        ExternalFunction function = null;
        try
        {
            function = m_uas.getDataSource(target_data_source_name).getExternalFunction(expr.getFunctionName());
        }
        catch (FunctionRegistryException e)
        {
            // Chain the exception
            throw new QueryCompilationException(QueryCompilation.INVALID_FUNCTION_CALL, expr.getLocation(), e);
        }
        catch (DataSourceException e)
        {
            // Chain the exception
            throw new QueryCompilationException(QueryCompilation.INVALID_FUNCTION_CALL, expr.getLocation(), e);
        }
        
        List<Term> args = new ArrayList<Term>();
        for (ValueExpression arg : expr.getArguments())
        {
            args.add(translateValueExpression(arg, op));
        }
        
        ExternalFunctionCall call = new ExternalFunctionCall(function, args);
        call.setLocation(expr.getLocation());
        call.inferType(op.getChildren());
        
        return call;
    }
    
    /**
     * Translates a general function node.
     * 
     * @param expr
     *            the node to translate
     * @param op
     *            the operator current translated
     * @return the translated term
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Term translateGeneralFunctionNode(GeneralFunctionNode expr, Operator op) throws QueryCompilationException
    {
        // Special cases for short-circuiting
        if (expr.getFunctionName().equals(AndFunction.NAME)) return translateAndFunctionNode(expr, op);
        if (expr.getFunctionName().equals(OrFunction.NAME)) return translateOrFunctionNode(expr, op);
        
        List<Term> args = new ArrayList<Term>();
        for (ValueExpression arg : expr.getArguments())
        {
            args.add(translateValueExpression(arg, op));
        }
        
        GeneralFunctionCall call = null;
        String name = expr.getFunctionName();
        try
        {
            call = new GeneralFunctionCall(name, args);
        }
        catch (FunctionRegistryException e)
        {
            // Chain the exception
            throw new QueryCompilationException(QueryCompilation.INVALID_FUNCTION_CALL, expr.getLocation(), e);
        }
        
        call.setLocation(expr.getLocation());
        call.inferType(op.getChildren());
        
        return call;
    }
    
    /**
     * Translates an AND function node.
     * 
     * @param expr
     *            the node to translate
     * @param op
     *            the operator current translated
     * @return the translated term
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Term translateAndFunctionNode(GeneralFunctionNode expr, Operator op) throws QueryCompilationException
    {
        assert (expr.getFunctionName().equals(AndFunction.NAME));
        List<Term> args = new ArrayList<Term>();
        assert (expr.getArguments().size() == 2);
        
        Term left = translateValueExpression(expr.getArguments().get(0), op);
        args.add(left);
        // The second argument is only evaluated if the first one is true
        m_branch_conditions.push(left);
        Term right = translateValueExpression(expr.getArguments().get(1), op);
        args.add(right);
        m_branch_conditions.pop();
        
        GeneralFunctionCall call = null;
        String name = expr.getFunctionName();
        try
        {
            call = new GeneralFunctionCall(name, args);
        }
        catch (FunctionRegistryException e)
        {
            // Chain the exception
            throw new QueryCompilationException(QueryCompilation.INVALID_FUNCTION_CALL, expr.getLocation(), e);
        }
        
        call.setLocation(expr.getLocation());
        call.inferType(op.getChildren());
        
        return call;
    }
    
    /**
     * Translates an OR function node.
     * 
     * @param expr
     *            the node to translate
     * @param op
     *            the operator current translated
     * @return the translated term
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Term translateOrFunctionNode(GeneralFunctionNode expr, Operator op) throws QueryCompilationException
    {
        assert (expr.getFunctionName().equals(OrFunction.NAME));
        List<Term> args = new ArrayList<Term>();
        assert (expr.getArguments().size() == 2);
        
        Term left = translateValueExpression(expr.getArguments().get(0), op);
        args.add(left);
        // The second argument is only evaluated if the first one is false or null
        Term null_left = new GeneralFunctionCall(new IsNullFunction(), left);
        Term not_left = new GeneralFunctionCall(new NotFunction(), left);
        Term or_left = new GeneralFunctionCall(new OrFunction(), null_left, not_left);
        m_branch_conditions.push(or_left);
        Term right = translateValueExpression(expr.getArguments().get(1), op);
        args.add(right);
        m_branch_conditions.pop();
        
        GeneralFunctionCall call = null;
        String name = expr.getFunctionName();
        try
        {
            call = new GeneralFunctionCall(name, args);
        }
        catch (FunctionRegistryException e)
        {
            // Chain the exception
            throw new QueryCompilationException(QueryCompilation.INVALID_FUNCTION_CALL, expr.getLocation(), e);
        }
        
        call.setLocation(expr.getLocation());
        call.inferType(op.getChildren());
        
        return call;
    }
    
    /**
     * Translates a NULL literal.
     * 
     * @param expr
     *            the node to translate
     * @param op
     *            the operator current translated
     * @return the translated term
     */
    private Term translateNullLiteral(NullLiteral expr, Operator op)
    {
        Constant constant = new Constant(new NullValue());
        constant.setLocation(expr.getLocation());
        constant.inferType(Collections.<Operator> emptyList());
        
        return constant;
    }
    
    /**
     * Translates a numeric literal.
     * 
     * @param expr
     *            the node to translate
     * @param op
     *            the operator current translated
     * @return the translated term
     */
    private Term translateNumericLiteral(NumericLiteral expr, Operator op)
    {
        Constant constant = new Constant(expr.getScalarValue());
        constant.setLocation(expr.getLocation());
        constant.inferType(op.getChildren());
        
        return constant;
    }
    
    /**
     * Translates a string literal.
     * 
     * @param expr
     *            the node to translate
     * @param op
     *            the operator current translated
     * @return the translated term
     */
    private Term translateStringLiteral(StringLiteral expr, Operator op)
    {
        Constant constant = new Constant(expr.getScalarValue());
        constant.setLocation(expr.getLocation());
        constant.inferType(Collections.<Operator> emptyList());
        
        return constant;
    }
    
    /**
     * Translates a collection (bag or list) literal.
     * 
     * @param expr
     *            the node to translate
     * @param op
     *            the operator current translated
     * @return the translated term
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Term translateCollectionFunctionNode(CollectionFunctionNode expr, Operator op) throws QueryCompilationException
    {
        List<Term> args = new ArrayList<Term>();
        for (ValueExpression item : expr.getElements())
        {
            args.add(translateValueExpression(item, op));
        }
        
        CollectionFunctionCall func_call = new CollectionFunctionCall(args);
        func_call.setLocation(expr.getLocation());
        func_call.setOrdered(expr.isOrdered());
        func_call.inferType(op.getChildren());
        
        return func_call;
    }
    
    /**
     * Translates a tuple literal.
     * 
     * @param expr
     *            the node to translate
     * @param op
     *            the operator current translated
     * @return the translated term
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private Term translateTupleFunctionNode(TupleFunctionNode expr, Operator op) throws QueryCompilationException
    {
        List<Term> args = new ArrayList<Term>();
        Set<String> aliases = new HashSet<String>();
        for (QueryConstruct item : expr.getTupleItems())
        {
            if (item instanceof TupleAllItem)
            {
                List<Pair<Term, Constant>> tuple_all_args = translateTupleAllItem((TupleAllItem) item, op);
                for (Pair<Term, Constant> pair : tuple_all_args)
                {
                    args.add(pair.getFirst());
                    args.add(pair.getSecond());
                    assert (pair.getSecond().getValue() instanceof StringValue);
                    if (!aliases.add(((StringValue) pair.getSecond().getValue()).getObject()))
                    {
                        throw new QueryCompilationException(QueryCompilation.DUPLICATE_TUPLE_ATTRIBUTE_NAME, item.getLocation(),
                                                            pair.getFirst());
                    }
                }
            }
            else
            {
                assert item instanceof TupleItem;
                TupleItem arg = (TupleItem) item;
                ValueExpression expression = arg.getExpression();
                if (expression instanceof SwitchNode)
                {
                    assert (op instanceof Project);
                    List<Pair<Term, String>> attributes = translateSwitchNode((SwitchNode) expression, arg.getAlias(),
                                                                              (Project) op);
                    for (Pair<Term, String> attr : attributes)
                    {
                        args.add(attr.getKey());
                        args.add(new Constant(new StringValue(attr.getValue())));
                        if (!aliases.add(attr.getValue()))
                        {
                            throw new QueryCompilationException(QueryCompilation.DUPLICATE_TUPLE_ATTRIBUTE_NAME,
                                                                item.getLocation(), attr.getFirst());
                        }
                    }
                }
                else
                {
                    args.add(translateValueExpression(expression, op));
                    Constant attr_name = new Constant(new StringValue(arg.getAlias()));
                    attr_name.setLocation(arg.getLocation());
                    args.add(attr_name);
                    if (!aliases.add(arg.getAlias()))
                    {
                        throw new QueryCompilationException(QueryCompilation.DUPLICATE_TUPLE_ATTRIBUTE_NAME,
                                                            expression.getLocation(), arg.getAlias());
                    }
                }
            }
        }
        
        TupleFunctionCall func_call = new TupleFunctionCall(args);
        func_call.setLocation(expr.getLocation());
        func_call.inferType(op.getChildren());
        
        return func_call;
    }
    
    /**
     * Translates a Switch node.
     * 
     * @param expr
     *            the switch to translate
     * @param expr_alias
     *            the alias of the switch node
     * @param op
     *            the project operator currently translated
     * @return the list of pairs of terms and corresponding aliases to add in the Project operator or TupleFunctionCall containing
     *         the Switch.
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private List<Pair<Term, String>> translateSwitchNode(SwitchNode expr, String expr_alias, Project op)
            throws QueryCompilationException
    {
        // We only support inline switch
        assert (expr.isInline());
        
        List<Pair<Term, String>> result_switch_terms = new ArrayList<Pair<Term, String>>();
        
        // For each iteration of the loop, we add the projection items corresponding to the current when branch and progressively
        // build the CASE WHEN function node that gives the selected branch.
        
        // The arguments for the CASE WHEN function node that gives the selected branch.
        List<Term> selected_branch_case_when_args = new ArrayList<Term>();
        
        for (OptionItem item : expr.getOptionItems())
        {
            ValueExpression when_expr = item.getWhenExpression();
            Term when_term = translateValueExpression(when_expr, op);
            when_term.inferType(op.getChildren());
            
            // The "then" branch is only executed if the "when" condition is true
            m_branch_conditions.push(when_term);
            
            // Continue translating the selected branch's CASE WHEN: if this branch is selected, output its alias.
            selected_branch_case_when_args.add(getExecutionConditionConjunct());
            selected_branch_case_when_args.add(new Constant(new StringValue(item.getAlias())));
            
            ValueExpression then_expr = item.getThenExpression();
            // We only support inline tuples inside inline switch
            if (then_expr instanceof TupleFunctionNode && ((TupleFunctionNode) then_expr).isInline())
            {
                TupleFunctionNode inline_tuple_expr = (TupleFunctionNode) then_expr;
                for (QueryConstruct tuple_arg : inline_tuple_expr.getTupleItems())
                {
                    assert (tuple_arg instanceof TupleItem);
                    TupleItem tuple_item = (TupleItem) tuple_arg;
                    String alias = tuple_item.getAlias();
                    ValueExpression tuple_item_expr = tuple_item.getExpression();
                    if (tuple_item_expr instanceof SwitchNode)
                    {
                        result_switch_terms.addAll(translateSwitchNode((SwitchNode) tuple_item_expr, tuple_item.getAlias(), op));
                    }
                    else
                    {
                        Term tuple_item_term = translateValueExpression(tuple_item_expr, op);
                        
                        List<Term> case_when_args = new ArrayList<Term>();
                        case_when_args.add(getExecutionConditionConjunct());
                        case_when_args.add(tuple_item_term);
                        
                        CaseFunctionCall func_call = new CaseFunctionCall(case_when_args);
                        func_call.setLocation(tuple_arg.getLocation());
                        func_call.inferType(op.getChildren());
                        
                        result_switch_terms.add(new Pair<Term, String>(func_call, alias));
                    }
                }
            }
            else if (then_expr instanceof SwitchNode && ((SwitchNode) then_expr).isInline())
            {
                Term then_term = translateValueExpression(then_expr, op);
                
                List<Term> case_when_args = new ArrayList<Term>();
                case_when_args.add(getExecutionConditionConjunct());
                case_when_args.add(then_term);
                
                CaseFunctionCall func_call = new CaseFunctionCall(case_when_args);
                func_call.setLocation(then_expr.getLocation());
                func_call.inferType(op.getChildren());
                
                result_switch_terms.add(new Pair<Term, String>(func_call, item.getAlias()));
            }
            else
            {
                throw new UnsupportedOperationException();
            }
            
            m_branch_conditions.pop();
            Term null_when = new GeneralFunctionCall(new IsNullFunction(), when_term);
            Term not_when = new GeneralFunctionCall(new NotFunction(), when_term);
            Term or_when = new GeneralFunctionCall(new OrFunction(), null_when, not_when);
            m_branch_conditions.push(or_when);
        }
        
        // Add the final CASE WHEN function node that gives the selected branch.
        CaseFunctionCall func_call = new CaseFunctionCall(selected_branch_case_when_args);
        func_call.setLocation(expr.getLocation());
        func_call.inferType(op.getChildren());
        
        result_switch_terms.add(new Pair<Term, String>(func_call, SwitchType.getSelectedCasePrefix() + expr_alias));
        
        // Remove all the conditions introduced by this switch function from the stack of conditions
        for (int i = 0; i < expr.getOptionItems().size(); i++)
        {
            m_branch_conditions.pop();
        }
        
        return result_switch_terms;
    }
    
    /**
     * Translates a tuple all item (t.*) inside a tuple literal.
     * 
     * @param expr
     *            the node to translate
     * @param op
     *            the operator current translated
     * @return the translated term
     * @throws QueryCompilationException
     *             if something goes wrong
     */
    private List<Pair<Term, Constant>> translateTupleAllItem(TupleAllItem expr, Operator op) throws QueryCompilationException
    {
        List<Pair<Term, Constant>> args = new ArrayList<Pair<Term, Constant>>();
        AttributeReference attr_ref = (AttributeReference) expr.getChildren().get(0);
        
        // Find the corresponding query path in the contexts, current or outer ones, by iterating over the contexts in reverse
        // order.
        ListIterator<LogicalPlanBuilderContext> iter = m_contexts.listIterator(m_contexts.size());
        boolean success = false;
        while (iter.hasPrevious())
        {
            LogicalPlanBuilderContext current_context = iter.previous();
            
            // Try to resolve the first step of the path in this context
            Term matched_term = current_context.resolveSuffix(attr_ref);
            
            if (matched_term != null)
            {
                // Success
                success = true;
                
                List<String> steps = new ArrayList<String>();
                if (matched_term instanceof RelativeVariable)
                {
                    steps.add(((RelativeVariable) matched_term).getName());
                }
                else
                {
                    steps.add(((RelativeVariable) ((QueryPath) matched_term).getTerm()).getName());
                    steps.addAll(((QueryPath) matched_term).getPathSteps());
                }
                // Find the type of the matched term
                Type matched_term_type = matched_term.getType();
                assert (matched_term_type instanceof TupleType);
                TupleType tuple_type = (TupleType) matched_term_type;
                
                // Find the attribute of the full path of attr_ref
                for (int i = 1; i < attr_ref.getPathSteps().size(); i++)
                {
                    String step = attr_ref.getPathSteps().get(i);
                    Type attr_type = tuple_type.getAttribute(step);
                    if (attr_type == null || !(attr_type instanceof TupleType))
                    {
                        throw new QueryCompilationException(QueryCompilation.UNKNOWN_ATTRIBUTE, attr_ref.getLocation(), attr_ref);
                    }
                    tuple_type = (TupleType) attr_type;
                    steps.add(step);
                }
                
                // Add all the attributes to the projection list
                for (String name : tuple_type.getAttributeNames())
                {
                    List<String> attr_steps = new ArrayList<String>(steps);
                    attr_steps.add(name);
                    AttributeReference temp_attr_ref = new AttributeReference(attr_steps, expr.getLocation());
                    Term item_term = translateValueExpression(temp_attr_ref, op);
                    item_term.setLocation(expr.getLocation());
                    
                    String project_alias = item_term.getDefaultProjectAlias();
                    Constant attr_name = new Constant(new StringValue(project_alias));
                    attr_name.setLocation(item_term.getLocation());
                    args.add(new Pair<Term, Constant>(item_term, attr_name));
                }
            }
            if (success) break;
        }
        if (!success)
        {
            throw new QueryCompilationException(QueryCompilation.UNKNOWN_ATTRIBUTE, attr_ref.getLocation(), attr_ref.toString());
        }
        
        return args;
    }
    
    /**
     * Returns the AND of all the execution conditions of the branch currently translated.
     * 
     * @return the conjunction of all the execution conditions
     */
    private Term getExecutionConditionConjunct()
    {
        if (m_branch_conditions.isEmpty())
        {
            return new Constant(new BooleanValue(true));
        }
        else
        {
            Term conjunct = m_branch_conditions.get(0);
            for (int i = 1; i < m_branch_conditions.size(); i++)
            {
                conjunct = new GeneralFunctionCall(new AndFunction(), Arrays.asList(conjunct, m_branch_conditions.get(i)));
            }
            return conjunct;
        }
    }
    
    /**
     * Gets the assign operator in all the contexts with respect to the target name.
     * 
     * @param target
     *            a specific assign target name.
     * @return the assign operator if found, <code>null</code> otherwise
     */
    private Assign getAssignByTarget(String target)
    {
        for (LogicalPlanBuilderContext context : m_contexts)
        {
            if (context.containsAssignTarget(target)) return context.getAssign(target);
        }
        return null;
    }
}
