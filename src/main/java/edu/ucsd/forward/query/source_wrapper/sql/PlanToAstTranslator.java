package edu.ucsd.forward.query.source_wrapper.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.jdbc.JdbcDataSource;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryParsingException;
import edu.ucsd.forward.query.ast.AstUtil;
import edu.ucsd.forward.query.ast.AttributeReference;
import edu.ucsd.forward.query.ast.FromExpressionItem;
import edu.ucsd.forward.query.ast.FromItem;
import edu.ucsd.forward.query.ast.GroupByItem;
import edu.ucsd.forward.query.ast.JoinItem;
import edu.ucsd.forward.query.ast.JoinType;
import edu.ucsd.forward.query.ast.OrderByItem;
import edu.ucsd.forward.query.ast.QueryExpression;
import edu.ucsd.forward.query.ast.QueryNode;
import edu.ucsd.forward.query.ast.QuerySpecification;
import edu.ucsd.forward.query.ast.QueryStatement;
import edu.ucsd.forward.query.ast.SelectExpressionItem;
import edu.ucsd.forward.query.ast.SetOpExpression;
import edu.ucsd.forward.query.ast.SetQuantifier;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.ast.WindowItem;
import edu.ucsd.forward.query.ast.WithItem;
import edu.ucsd.forward.query.ast.function.AggregateFunctionNode;
import edu.ucsd.forward.query.ast.function.AggregateFunctionNode.AggregateType;
import edu.ucsd.forward.query.ast.function.CaseFunctionNode;
import edu.ucsd.forward.query.ast.function.CastFunctionNode;
import edu.ucsd.forward.query.ast.function.ExternalFunctionNode;
import edu.ucsd.forward.query.ast.function.GeneralFunctionNode;
import edu.ucsd.forward.query.ast.literal.Literal;
import edu.ucsd.forward.query.function.aggregate.AggregateFunctionCall;
import edu.ucsd.forward.query.function.cast.CastFunctionCall;
import edu.ucsd.forward.query.function.conditional.CaseFunctionCall;
import edu.ucsd.forward.query.function.external.ExternalFunctionCall;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.logical.AbstractUnaryOperator;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.logical.EliminateDuplicates;
import edu.ucsd.forward.query.logical.Exists;
import edu.ucsd.forward.query.logical.Ground;
import edu.ucsd.forward.query.logical.GroupBy;
import edu.ucsd.forward.query.logical.SendPlan;
import edu.ucsd.forward.query.logical.Subquery;
import edu.ucsd.forward.query.logical.InnerJoin;
import edu.ucsd.forward.query.logical.Navigate;
import edu.ucsd.forward.query.logical.OffsetFetch;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.OuterJoin;
import edu.ucsd.forward.query.logical.PartitionBy;
import edu.ucsd.forward.query.logical.Product;
import edu.ucsd.forward.query.logical.Project;
import edu.ucsd.forward.query.logical.Project.Item;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.Select;
import edu.ucsd.forward.query.logical.SetOperator;
import edu.ucsd.forward.query.logical.Sort;
import edu.ucsd.forward.query.logical.UnaryOperator;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.plan.LogicalPlanUtil;
import edu.ucsd.forward.query.logical.term.AbsoluteVariable;
import edu.ucsd.forward.query.logical.term.Constant;
import edu.ucsd.forward.query.logical.term.ElementVariable;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.physical.ParameterEvaluator;
import edu.ucsd.forward.query.physical.QueryPathEvaluator;
import edu.ucsd.forward.query.physical.VariableEvaluator;

/**
 * The module that translates a SQL-compliant logical plan to an AST. This translator is used by send plan operator implementation
 * to send the translated SQL query to a JDBC data source for evaluation. Note that every nested send-plan operator is translated
 * into a FROM item which references the transient data object that captures the intermediate evaluation result.
 * 
 * @author Romain Vernoux
 */
public class PlanToAstTranslator
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(PlanToAstTranslator.class);
    
    private DataSource          m_execution_data_source;
    
    /**
     * Default constructor.
     */
    public PlanToAstTranslator()
    {
        
    }
    
    /**
     * Tanslates the given logical plan into the AST.
     * 
     * @param plan
     *            the logical plan to translate
     * @param data_source
     *            the data source that the plan executes on
     * @return the root node of the translated AST tree.
     */
    public QueryStatement translate(LogicalPlan plan, DataSource data_source)
    {
        assert (data_source != null);
        m_execution_data_source = data_source;
        
        // Translate the WITH statements.
        List<WithItem> with_items = new ArrayList<WithItem>();
        for (Assign assign : plan.getAssigns())
        {
            LogicalPlan assign_plan = assign.getPlan();
            assert (assign_plan.getRootOperator() instanceof SendPlan);
            assign_plan = ((SendPlan) assign_plan.getRootOperator()).getSendPlan();
            PlanToAstTranslator translator = new PlanToAstTranslator();
            QueryStatement assign_statement = translator.translate(assign_plan, data_source);
            assert assign_statement instanceof QueryNode;
            WithItem with_item = new WithItem(assign.getTarget(), (QueryNode) assign_statement,
                                              new LocationImpl(Location.UNKNOWN_PATH));
            with_items.add(with_item);
        }
        
        Operator root_op = plan.getRootOperator();
        QueryExpression result = translateQuery(root_op);
        result.setWithItems(with_items);
        
        return result;
    }
    
    /**
     * Tanslates the given tree of operators into the AST.
     * 
     * @param root_op
     *            the root operator of the plan to translate.
     * @return the root node of the translated AST tree.
     */
    private QueryExpression translateQuery(Operator root_op)
    {
        QueryExpression result = null;
        if (root_op instanceof OffsetFetch || root_op instanceof Sort)
        {
            QueryExpression query_expr = getQueryExpression(root_op);
            result = query_expr;
        }
        else if (root_op instanceof SetOperator)
        {
            SetOpExpression setop_expr = getSetOpExpression((SetOperator) root_op);
            QueryExpression query_expr = new QueryExpression(new LocationImpl(Location.UNKNOWN_PATH));
            try
            {
                query_expr.setBody(setop_expr);
            }
            catch (QueryParsingException e)
            {
                throw new AssertionError(e);
            }
            result = query_expr;
        }
        else if (root_op instanceof EliminateDuplicates || root_op instanceof Project)
        {
            QueryExpression query_expr = getQueryExpression(root_op);
            result = query_expr;
        }
        else
        {
            throw new UnsupportedOperationException();
        }
        
        return result;
    }
    
    /**
     * Translates a tree of operators into a query expression.
     * 
     * @param root_op
     *            the root of the tree of operators.
     * @return the translated query expression
     */
    private QueryExpression getQueryExpression(Operator root_op)
    {
        Operator op = root_op;
        QueryExpression query_expr = new QueryExpression(new LocationImpl(Location.UNKNOWN_PATH));
        if (op instanceof OffsetFetch)
        {
            translateOffsetFetch(query_expr, (OffsetFetch) op);
            op = LogicalPlanUtil.getNextClauseOperator(op);
        }
        if (op instanceof Sort)
        {
            translateSort(query_expr, (Sort) op);
            op = LogicalPlanUtil.getNextClauseOperator(op);
        }
        if (op instanceof SetOperator)
        {
            SetOpExpression setop_expr = getSetOpExpression((SetOperator) op);
            try
            {
                query_expr.setBody(setop_expr);
            }
            catch (QueryParsingException e)
            {
                throw new AssertionError(e);
            }
        }
        else if (op instanceof EliminateDuplicates || op instanceof Project)
        {
            // First translate the ORDER BY/LIMIT clauses
            Operator sort_or_limit = op;
            if (sort_or_limit instanceof EliminateDuplicates)
            {
                sort_or_limit = LogicalPlanUtil.getNextClauseOperator(sort_or_limit);
            }
            assert (sort_or_limit instanceof Project);
            sort_or_limit = LogicalPlanUtil.getNextClauseOperator(sort_or_limit);
            if (sort_or_limit instanceof OffsetFetch)
            {
                translateOffsetFetch(query_expr, (OffsetFetch) sort_or_limit);
                sort_or_limit = LogicalPlanUtil.getNextClauseOperator(sort_or_limit);
            }
            if (sort_or_limit instanceof Sort)
            {
                translateSort(query_expr, (Sort) sort_or_limit);
                sort_or_limit = LogicalPlanUtil.getNextClauseOperator(sort_or_limit);
            }
            // Then translate the rest of the query specfication
            QuerySpecification query_spec = getQuerySpecification(op);
            try
            {
                query_expr.setBody(query_spec);
            }
            catch (QueryParsingException e)
            {
                throw new AssertionError(e);
            }
        }
        return query_expr;
    }
    
    /**
     * Translates a tree of operators into a set operator expression.
     * 
     * @param root_op
     *            the root of the tree of operators. Has to be a SetOperator
     * @return the translated set operator expression
     */
    private SetOpExpression getSetOpExpression(SetOperator root_op)
    {
        // These are the only supported set operations
        switch (root_op.getSetOpType())
        {
            case UNION:
                break;
            case INTERSECT:
                break;
            case EXCEPT:
                break;
            default:
                throw new UnsupportedOperationException();
        }
        
        QueryNode left_query_node = null;
        QueryNode right_query_node = null;
        
        // Translate the left child
        Operator left_child = root_op.getChildren().get(0);
        if (left_child instanceof SetOperator)
        {
            left_query_node = getSetOpExpression((SetOperator) left_child);
        }
        else if (left_child instanceof EliminateDuplicates || left_child instanceof Project)
        {
            left_query_node = getQueryExpression(left_child);
        }
        else
        {
            throw new UnsupportedOperationException();
        }
        
        // Translate the right child
        Operator right_child = root_op.getChildren().get(1);
        if (right_child instanceof SetOperator)
        {
            right_query_node = getSetOpExpression((SetOperator) right_child);
        }
        else if (right_child instanceof EliminateDuplicates || right_child instanceof Project)
        {
            right_query_node = getQueryExpression(right_child);
        }
        else
        {
            throw new UnsupportedOperationException();
        }
        
        SetOpExpression set_op_query = new SetOpExpression(root_op.getSetOpType(), root_op.getSetQuantifier(), left_query_node,
                                                           right_query_node, new LocationImpl(Location.UNKNOWN_PATH));
        return set_op_query;
    }
    
    /**
     * Translates a tree of operators into a query specification.
     * 
     * @param root_op
     *            the root of the tree of operators.
     * @return the translated query specification
     */
    private QuerySpecification getQuerySpecification(Operator root_op)
    {
        QuerySpecification query_spec = new QuerySpecification(new LocationImpl(Location.UNKNOWN_PATH));
        
        Operator op = root_op;
        if (op instanceof EliminateDuplicates)
        {
            translateEliminateDuplicates(query_spec, (EliminateDuplicates) op);
            op = LogicalPlanUtil.getNextClauseOperator(op);
        }
        assert (op instanceof Project);
        translateProject(query_spec, (Project) op);
        op = LogicalPlanUtil.getNextClauseOperator(op);
        // Skip the OffsetFetch that has been translated before
        if (op instanceof OffsetFetch)
        {
            op = LogicalPlanUtil.getNextClauseOperator(op);
        }
        // Skip the Sort that has been translated before
        if (op instanceof Sort)
        {
            op = LogicalPlanUtil.getNextClauseOperator(op);
        }
        if (op instanceof PartitionBy)
        {
            translatePartitionBy(query_spec, (PartitionBy) op);
            op = LogicalPlanUtil.getNextClauseOperator(op);
        }
        if (op instanceof Select)
        {
            if (LogicalPlanUtil.getNextClauseOperator(op) instanceof GroupBy)
            {
                translateSelectHaving(query_spec, (Select) op);
            }
            else
            {
                translateSelectWhere(query_spec, (Select) op);
            }
            op = LogicalPlanUtil.getNextClauseOperator(op);
        }
        if (op instanceof GroupBy)
        {
            translateGroupBy(query_spec, (GroupBy) op);
            op = LogicalPlanUtil.getNextClauseOperator(op);
        }
        if (op instanceof Select)
        {
            translateSelectWhere(query_spec, (Select) op);
            op = LogicalPlanUtil.getNextClauseOperator(op);
        }
        if (op instanceof Product)
        {
            for (Operator child : op.getChildren())
            {
                FromItem from_item = translateFromItem(child);
                query_spec.addFromItem(from_item);
            }
        }
        else if (!(op instanceof Ground))
        {
            FromItem from_item = translateFromItem(op);
            query_spec.addFromItem(from_item);
        }
        
        return query_spec;
    }
    
    /**
     * Translates a tree of operatrs into a FROM item.
     * 
     * @param op
     *            the root of the operator tree to translate. Has to be a Scan, a Subquery or a join.
     * @return the translated from item
     */
    private FromItem translateFromItem(Operator op)
    {
        FromItem from_item = null;
        if (op instanceof Scan)
        {
            from_item = translateScan((Scan) op);
            assert (((Scan) op).getChild() instanceof Ground);
        }
        else if (op instanceof OuterJoin)
        {
            JoinType join_type = null;
            switch (((OuterJoin) op).getOuterJoinVariation())
            {
                case FULL:
                    join_type = JoinType.FULL_OUTER;
                    break;
                case LEFT:
                    join_type = JoinType.LEFT_OUTER;
                    break;
                case RIGHT:
                    join_type = JoinType.RIGHT_OUTER;
                    break;
            }
            from_item = translateJoin((OuterJoin) op, join_type, ((OuterJoin) op).getMergedCondition());
        }
        else if (op instanceof InnerJoin)
        {
            from_item = translateJoin((InnerJoin) op, JoinType.INNER, ((InnerJoin) op).getMergedCondition());
        }
        else if (op instanceof Subquery)
        {
            from_item = translateSubquery((Subquery) op);
        }
        else
        {
            throw new UnsupportedOperationException();
        }
        return from_item;
    }
    
    /**
     * Translates a Subquery operator.
     * 
     * @param operator
     *            the operator to translate
     * @return the translated from item
     */
    private FromExpressionItem translateSubquery(Subquery operator)
    {
        Operator child_op = operator.getChild();
        
        QueryExpression child_query = translateQuery(child_op);
        
        FromExpressionItem from_item = new FromExpressionItem(child_query, new LocationImpl(Location.UNKNOWN_PATH));
        from_item.setAlias(operator.getAliasVariable().getName());
        
        return from_item;
    }
    
    /**
     * Translates a Scan operator.
     * 
     * @param operator
     *            the operator to translate
     * @return the translated from item
     */
    private FromExpressionItem translateScan(Scan operator)
    {
        ValueExpression node = null;
        if (operator.getTerm() instanceof Variable)
        {
            node = this.translateTerm(operator.getTerm(), operator);
        }
        else if (operator.getTerm() instanceof Parameter)
        {
            // Constructs the reference
            node = this.translateTerm(operator.getTerm(), operator);
            
            // Romain: suspicious piece of code, commented out for now.
            // if (((Parameter) operator.getTerm()).getInstantiationMethod().equals(Parameter.InstantiationMethod.ASSIGN))
            // {
            // FromExpressionItem item = new FromExpressionItem(node, new LocationImpl(Location.UNKNOWN_PATH));
            // List<String> path = ((AttributeReference) node).getPathSteps();
            // ((AttributeReference) node).setPathSteps(path.subList(0, 1));
            // String alias = operator.getAliasVariable().getName() + path.get(1);
            // item.setAlias(alias);
            // return item;
            // }
        }
        else if (operator.getTerm() instanceof QueryPath)
        {
            node = this.translateTerm(operator.getTerm(), operator);
        }
        else if (operator.getTerm() instanceof ExternalFunctionCall)
        {
            node = this.translateTerm(operator.getTerm(), operator);
        }
        else
        {
            throw new UnsupportedOperationException();
        }
        
        FromExpressionItem from_item = new FromExpressionItem(node, new LocationImpl(Location.UNKNOWN_PATH));
        from_item.setAlias(operator.getAliasVariable().getName());
        
        return from_item;
    }
    
    /**
     * Translates an OffsetFetch operator.
     * 
     * @param query_expr
     *            the query expression currently translated
     * @param operator
     *            the OffsetFetch operator to translate
     */
    private void translateOffsetFetch(QueryExpression query_expr, OffsetFetch operator)
    {
        try
        {
            if (operator.getOffset() != null)
            {
                query_expr.setOffset(translateTerm(operator.getOffset(), operator));
            }
            
            if (operator.getFetch() != null)
            {
                query_expr.setFetch(translateTerm(operator.getFetch(), operator));
            }
        }
        catch (QueryParsingException e)
        {
            throw new AssertionError(e);
        }
    }
    
    /**
     * Translates a Sort operator.
     * 
     * @param query_expr
     *            the query expression currently translated
     * @param operator
     *            the Sort operator to translate
     */
    private void translateSort(QueryExpression query_expr, Sort operator)
    {
        List<OrderByItem> order_items = new ArrayList<OrderByItem>();
        for (Sort.Item sort_item : operator.getSortItems())
        {
            ValueExpression sort_exp = translateTerm(sort_item.getTerm(), operator);
            OrderByItem order_item = new OrderByItem(sort_exp, sort_item.getSpec(), sort_item.getNulls(),
                                                     new LocationImpl(Location.UNKNOWN_PATH));
            order_items.add(order_item);
        }
        try
        {
            query_expr.setOrderByItems(order_items);
        }
        catch (QueryParsingException e)
        {
            throw new AssertionError(e);
        }
    }
    
    /**
     * Translates an EliminateDuplicates operator.
     * 
     * @param query_spec
     *            the query specification currently translated
     * @param operator
     *            the EliminateDuplicates operator to translate
     */
    private void translateEliminateDuplicates(QuerySpecification query_spec, EliminateDuplicates operator)
    {
        query_spec.setSetQuantifier(SetQuantifier.DISTINCT);
    }
    
    /**
     * Translates a Project operator.
     * 
     * @param query_spec
     *            the query specification currently translated
     * @param operator
     *            the Project operator to translate
     */
    private void translateProject(QuerySpecification query_spec, Project operator)
    {
        for (Item item : operator.getProjectionItems())
        {
            ValueExpression expression = translateTerm(item.getTerm(), operator);
            
            // If the expression is a non-null constant, then wrap it with a CAST function call, because the underlying data source
            // might not be able to infer its type.
            if (expression instanceof Literal)
            {
                assert item.getTerm().getType() != null;
                String type_name = TypeEnum.getName(item.getTerm().getType());
                expression = new CastFunctionNode(expression, type_name, new LocationImpl(Location.UNKNOWN_PATH));
            }
            
            SelectExpressionItem select_item = new SelectExpressionItem(expression, new LocationImpl(Location.UNKNOWN_PATH));
            select_item.setAlias(item.getAlias());
            
            query_spec.addSelectItem(select_item);
        }
    }
    
    /**
     * Translates a PartitionBy operator.
     * 
     * @param query_spec
     *            the query specification currently translated
     * @param operator
     *            the PartitionBy operator to translate
     */
    private void translatePartitionBy(QuerySpecification query_spec, PartitionBy operator)
    {
        // Construct window item
        WindowItem window_item = new WindowItem(new LocationImpl(Location.UNKNOWN_PATH));
        for (RelativeVariable var : operator.getPartitionByTerms())
        {
            GroupByItem item = new GroupByItem(new LocationImpl(Location.UNKNOWN_PATH));
            ValueExpression expression = translateTerm(var, operator);
            item.setExpression(expression);
            window_item.addPartitionByItem(item);
        }
        List<OrderByItem> order_by_items = new ArrayList<OrderByItem>();
        if (!operator.getSortByItems().isEmpty())
        {
            for (Sort.Item item : operator.getSortByItems())
            {
                assert (item.getTerm() instanceof RelativeVariable);
                ValueExpression expression = translateTerm(item.getTerm(), operator);
                order_by_items.add(new OrderByItem(expression, null, null, new LocationImpl(Location.UNKNOWN_PATH)));
            }
            try
            {
                window_item.setOrderByItems(order_by_items);
            }
            catch (QueryParsingException e)
            {
                throw new AssertionError(e);
            }
        }
        query_spec.setWindowItem(window_item);
    }
    
    /**
     * Translates a Select operator corresponding to the HAVING clause.
     * 
     * @param query_spec
     *            the query specification currently translated
     * @param operator
     *            the Select operator to translate
     */
    private void translateSelectHaving(QuerySpecification query_spec, Select operator)
    {
        ValueExpression condition = translateTerm(operator.getMergedCondition(), operator);
        query_spec.setHavingExpression(condition);
    }
    
    /**
     * Translates a Select operator corresponding to the WHERE clause.
     * 
     * @param query_spec
     *            the query specification currently translated
     * @param operator
     *            the Select operator to translate
     */
    private void translateSelectWhere(QuerySpecification query_spec, Select operator)
    {
        ValueExpression condition = translateTerm(operator.getMergedCondition(), operator);
        query_spec.setWhereExpression(condition);
    }
    
    /**
     * Translates a GroupBy operator.
     * 
     * @param query_spec
     *            the query specification currently translated
     * @param operator
     *            the GroupBy operator to translate
     */
    private void translateGroupBy(QuerySpecification query_spec, GroupBy operator)
    {
        for (Term term : operator.getGroupByTerms())
        {
            GroupByItem group_element = new GroupByItem(new LocationImpl(Location.UNKNOWN_PATH));
            group_element.setExpression(translateTerm(term, operator));
            query_spec.addGroupByItem(group_element);
        }
    }
    
    /**
     * Translates a join operator.
     * 
     * @param operator
     *            the join operator to translate.
     * @param type
     *            the join type
     * @param condition
     *            the join condition
     * @return the translater join item
     */
    private JoinItem translateJoin(Operator operator, JoinType type, Term condition)
    {
        List<FromItem> items = new ArrayList<FromItem>(2);
        assert (operator.getChildren().size() == 2);
        for (Operator child : operator.getChildren())
        {
            // Skip the Navigate and Exists correspondings to the condition
            Operator op = child;
            while (op instanceof Navigate || op instanceof Exists)
            {
                op = ((AbstractUnaryOperator) op).getChild();
            }
            items.add(translateFromItem(op));
        }
        
        // Constructs the JOIN item
        JoinItem join_item = new JoinItem(items.get(0), items.get(1), type, new LocationImpl(Location.UNKNOWN_PATH));
        
        // Add JOIN condition
        if (condition != null)
        {
            join_item.setOnCondition(translateTerm(condition, operator));
        }
        
        return join_item;
    }
    
    /**
     * Translates a term.
     * 
     * @param term
     *            the term to translate
     * @param operator
     *            the operator using the term
     * @return the translated value expression
     */
    private ValueExpression translateTerm(Term term, Operator operator)
    {
        if (term instanceof Constant)
        {
            return AstUtil.translateConstant(((Constant) term).getValue());
        }
        else if (term instanceof Parameter)
        {
            Parameter param = (Parameter) term;
            
            switch (param.getInstantiationMethod())
            {
                case ASSIGN:
                    Value param_value = null;
                    try
                    {
                        param_value = ParameterEvaluator.evaluate(param, null).getValue();
                    }
                    catch (QueryExecutionException e)
                    {
                        throw new AssertionError(e);
                    }
                    
                    if (param.getType() instanceof CollectionType)
                    {
                        throw new AssertionError();
                        // Romain: suspicious code. Commented out for now.
                        // Type child_type = ((CollectionType) param.getType()).getChildrenType();
                        // assert (child_type instanceof TupleType);
                        // TupleType tuple_type = (TupleType) child_type;
                        //
                        // StringBuilder sb_schema = new StringBuilder(" (");
                        // for (String name : tuple_type.getAttributeNames())
                        // {
                        // sb_schema.append(name + ",");
                        // }
                        // sb_schema.delete(sb_schema.length() - 1, sb_schema.length());
                        // sb_schema.append(")");
                        //
                        // AttributeReference att = (AttributeReference) translateValue(param_value);
                        // List<String> steps = new ArrayList<String>();
                        // steps.add(att.getPathSteps().get(0));
                        // steps.add(sb_schema.toString());
                        // AttributeReference new_att = new AttributeReference(steps, new LocationImpl(Location.UNKNOWN_PATH));
                        // return new_att;
                    }
                    return AstUtil.translateValue(param_value);
                    
                case COPY:
                    // Since the schema object is temporary, there is no need to mention the schema name
                    return new AttributeReference(Collections.singletonList(param.getSchemaObjectName()),
                                                  new LocationImpl(Location.UNKNOWN_PATH));
            }
        }
        else if (term instanceof QueryPath)
        {
            QueryPath path = (QueryPath) term;
            
            if (!(path.getTerm() instanceof RelativeVariable))
            {
                // Evaluate the QueryPath and plug in the value
                Value value = null;
                try
                {
                    value = QueryPathEvaluator.evaluate(path, null).getValue();
                }
                catch (QueryExecutionException e)
                {
                    throw new AssertionError(e);
                }
                return AstUtil.translateValue(value);
            }
            else if (operator instanceof Sort && ((RelativeVariable) path.getTerm()).getName().equals(Project.PROJECT_ALIAS))
            {
                // A sort above a project contains a query path starting with the default project alias
                assert (path.getPathSteps().size() == 1);
                // Discard the first alias and return the name of the project item
                return new AttributeReference(path.getPathSteps(), new LocationImpl(Location.UNKNOWN_PATH));
            }
            else if (path.getTerm() instanceof ElementVariable)
            {
                // Path starting from the subquery alias or a Scan alias
                assert (path.getPathSteps().size() == 1);
                List<String> path_steps = new ArrayList<String>(2);
                path_steps.add(((ElementVariable) path.getTerm()).getName());
                path_steps.add(path.getPathSteps().get(0));
                return new AttributeReference(path_steps, new LocationImpl(Location.UNKNOWN_PATH));
            }
            else
            {
                throw new AssertionError();
            }
        }
        else if (term instanceof AbsoluteVariable)
        {
            AbsoluteVariable var = (AbsoluteVariable) term;
            
            // If the variable refers to the temp data source for assigned attributes, then return an attribute reference
            if (var.getDataSourceName().equals(DataSource.TEMP_ASSIGN_SOURCE))
            {
                return new AttributeReference(Arrays.asList(var.getSchemaObjectName()), new LocationImpl(Location.UNKNOWN_PATH));
            }
            // If the variable refers to the data source that will execute the query, then return an attribute reference
            else if (var.getDataSourceName().equals(m_execution_data_source.getMetaData().getName()))
            {
                // Assume the referenced schema object is persistent
                JdbcDataSource jdbc_data_source = (JdbcDataSource) m_execution_data_source;
                
                // Since the scan will occur in a JDBC data source, replace the source name with the schema name
                String schema_name = jdbc_data_source.getMetaData().getSchemaName();
                String[] steps = { schema_name, var.getSchemaObjectName() };
                
                return new AttributeReference(Arrays.asList(steps), new LocationImpl(Location.UNKNOWN_PATH));
            }
            // Otherwise, evaluate the variable and plug in the value
            else
            {
                Value value = null;
                try
                {
                    value = VariableEvaluator.evaluate(var, null).getValue();
                }
                catch (QueryExecutionException e)
                {
                    throw new AssertionError(e);
                }
                
                return AstUtil.translateValue(value);
            }
        }
        else if (term instanceof RelativeVariable)
        {
            RelativeVariable var = (RelativeVariable) term;
            
            // Find the operator that outputs this variable
            Operator var_op = findChildOperatorWithVariable(var, operator);
            assert(var_op != null);
            
            if (var_op instanceof Navigate)
            {
                Navigate nav = (Navigate) var_op;
                
                if (nav.getTerm() instanceof QueryPath)
                {
                    return this.translateTerm(nav.getTerm(), operator);
                }
                else if (nav.getTerm() instanceof AbsoluteVariable)
                {
                    return this.translateTerm(nav.getTerm(), operator);
                }
                else
                {
                    throw new AssertionError();
                }
            }
            else if (var_op instanceof GroupBy)
            {
                GroupBy group_by = (GroupBy) var_op;
                // Look into the grouping terms
                for (GroupBy.Item group_item : group_by.getGroupByItems())
                {
                    if (group_item.getVariable().equals(var))
                    {
                        return this.translateTerm(group_item.getTerm(), group_by);
                    }
                }
                // Look into the aggregate functions
                for (GroupBy.Aggregate agg : group_by.getAggregates())
                {
                    if (agg.getAlias().equals(var.getName()))
                    {
                        return this.translateAggregateFunctionCall(agg.getAggregateFunctionCall(), group_by);
                    }
                }
            }
            else if (var_op instanceof Exists)
            {
                // Not supported for now because we don't push exists.
                throw new UnsupportedOperationException();
            }
            else
            {
                throw new UnsupportedOperationException();
            }
        }
        else if (term instanceof ExternalFunctionCall)
        {
            ExternalFunctionCall call = (ExternalFunctionCall) term;
            List<ValueExpression> arguments = new ArrayList<ValueExpression>();
            for (Term argu : call.getArguments())
            {
                arguments.add(translateTerm(argu, operator));
            }
            
            // Since the scan will occur in a JDBC data source, replace the source name with the schema name
            String schema = ((JdbcDataSource) m_execution_data_source).getMetaData().getSchemaName();
            ExternalFunctionNode node = new ExternalFunctionNode(call.getFunction().getName(), schema,
                                                                 new LocationImpl(Location.UNKNOWN_PATH));
            node.addArguments(arguments);
            
            return node;
        }
        else if (term instanceof GeneralFunctionCall)
        {
            GeneralFunctionCall call = (GeneralFunctionCall) term;
            List<ValueExpression> arguments = new ArrayList<ValueExpression>();
            for (Term arg : call.getArguments())
            {
                arguments.add(translateTerm(arg, operator));
            }
            
            GeneralFunctionNode node = new GeneralFunctionNode(call.getFunction().getName(),
                                                               new LocationImpl(Location.UNKNOWN_PATH));
            node.addArguments(arguments);
            
            return node;
        }
        else if (term instanceof CastFunctionCall)
        {
            CastFunctionCall call = (CastFunctionCall) term;
            ValueExpression argument = translateTerm(call.getArguments().get(0), operator);
            assert (call.getTargetType().get() instanceof ScalarType);
            CastFunctionNode node = new CastFunctionNode(argument, call.getTargetType().getName(),
                                                         new LocationImpl(Location.UNKNOWN_PATH));
            
            return node;
        }
        else if (term instanceof CaseFunctionCall)
        {
            CaseFunctionCall call = (CaseFunctionCall) term;
            List<ValueExpression> arguments = new ArrayList<ValueExpression>();
            for (Term arg : call.getArguments())
            {
                arguments.add(translateTerm(arg, operator));
            }
            
            CaseFunctionNode node = new CaseFunctionNode(arguments, new LocationImpl(Location.UNKNOWN_PATH));
            
            return node;
        }
        
        throw new UnsupportedOperationException();
    }
    
    
    
    
    
    /**
     * Translates the aggregate function call into aggregate function node.
     * 
     * @param call
     *            the aggregate function call to be translated
     * @param operator
     *            the operator that the function call is from
     * @return the translated aggregate function node
     */
    private AggregateFunctionNode translateAggregateFunctionCall(AggregateFunctionCall call, Operator operator)
    {
        AggregateType type = AggregateType.valueOf(call.getFunction().getName());
        assert type != null;
        ValueExpression argument;
        if (call.getArguments().size() > 0)
        {
            argument = translateTerm(call.getArguments().get(0), operator);
            return new AggregateFunctionNode(type, call.getSetQuantifier(), Arrays.asList(argument),
                                             new LocationImpl(Location.UNKNOWN_PATH));
        }
        // Accommodate COUNT(*)
        else
        {
            return new AggregateFunctionNode(type, new LocationImpl(Location.UNKNOWN_PATH));
        }
    }
    
    /**
     * Finds the operator below a given root operator that outputs a given variable. Uses normal forms assumptions to find the
     * operator.
     * 
     * @param var
     *            the variable to look for
     * @param root_op
     *            the root operator of the tree to look into
     * @return the operator that outputs the given variable, or <code>null</code> if none is found
     */
    private Operator findChildOperatorWithVariable(RelativeVariable var, Operator root_op)
    {
        if (root_op instanceof UnaryOperator)
        {
            Operator op = ((UnaryOperator) root_op).getChild();
            return findOperatorWithVariable(var, op);
        }
        else if (root_op.getChildren().size() == 2)
        {
            // Accomodate joins
            Operator left_child = root_op.getChildren().get(0);
            Operator right_child = root_op.getChildren().get(1);
            Operator result = null;
            if (left_child instanceof Navigate || left_child instanceof Exists)
            {
                result = findOperatorWithVariable(var, left_child);
            }
            if (result == null && (right_child instanceof Navigate || right_child instanceof Exists))
            {
                result = findOperatorWithVariable(var, right_child);
            }
            return result;
        }
        else
        {
            return null;
        }
    }
    
    /**
     * Finds the operator below a given root operator (included) that outputs a given variable. Uses normal forms assumptions to
     * find the operator.
     * 
     * @param var
     *            the variable to look for
     * @param root_op
     *            the root operator of the tree to look into
     * @return the operator that outputs the given variable, or <code>null</code> if none is found
     */
    private Operator findOperatorWithVariable(RelativeVariable var, Operator root_op)
    {
        Operator op = root_op;
        while (true)
        {
            if (op instanceof Navigate)
            {
                if (((Navigate) op).getAliasVariable().equals(var))
                {
                    return op;
                }
                else
                {
                    op = ((Navigate) op).getChild();
                }
            }
            else if (op instanceof Exists)
            {
                // Not supported for now because we don't push exists.
                throw new UnsupportedOperationException();
                // if (((Exists) op).getVariableName().equals(var.getName()))
                // {
                // return op;
                // }
                // else
                // {
                // op = ((Exists) op).getChild();
                // }
            }
            else if (op instanceof Sort)
            {
                // Since the Sort operator can use the same variables as the Project operator, when we translate the terms of the
                // Project operator, we need to cross the Sort and look below
                op = ((Sort) op).getChild();
            }
            else if (op instanceof OffsetFetch)
            {
                // Since the OffsetFetch is above the Sort, we also need to look below
                op = ((OffsetFetch) op).getChild();
            }
            else if (op instanceof Select && LogicalPlanUtil.getNextClauseOperator(op) instanceof GroupBy)
            {
                // For the same reasons, we need to look below the Select that corresponds to the HAVING clause
                op = ((Select) op).getChild();
            }
            else if (op instanceof GroupBy)
            {
                GroupBy group_by = (GroupBy) op;
                // Look into the grouping terms
                for (GroupBy.Item group_item : group_by.getGroupByItems())
                {
                    if (group_item.getVariable().equals(var))
                    {
                        return group_by;
                    }
                }
                // Look into the aggregate functions
                for (GroupBy.Aggregate agg : group_by.getAggregates())
                {
                    if (agg.getAlias().equals(var.getName()))
                    {
                        return group_by;
                    }
                }
                // Don't go below GroupBy
                break;
            }
            else
            {
                // Stop here
                break;
            }
        }
        return null;
    }
}
