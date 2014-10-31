/**
 * 
 */
package edu.ucsd.forward.query.xml;

import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.data.xml.TypeXmlSerializer;
import edu.ucsd.forward.query.ast.GroupByItem;
import edu.ucsd.forward.query.function.FunctionCall;
import edu.ucsd.forward.query.function.aggregate.AggregateFunctionCall;
import edu.ucsd.forward.query.function.cast.CastFunctionCall;
import edu.ucsd.forward.query.function.external.ExternalFunctionCall;
import edu.ucsd.forward.query.logical.AntiSemiJoin;
import edu.ucsd.forward.query.logical.ApplyPlan;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;
import edu.ucsd.forward.query.logical.Copy;
import edu.ucsd.forward.query.logical.Exists;
import edu.ucsd.forward.query.logical.Ground;
import edu.ucsd.forward.query.logical.GroupBy;
import edu.ucsd.forward.query.logical.GroupBy.Aggregate;
import edu.ucsd.forward.query.logical.IndexScan;
import edu.ucsd.forward.query.logical.IndexScan.KeyRangeSpec;
import edu.ucsd.forward.query.logical.InnerJoin;
import edu.ucsd.forward.query.logical.Navigate;
import edu.ucsd.forward.query.logical.OffsetFetch;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.OuterJoin;
import edu.ucsd.forward.query.logical.PartitionBy;
import edu.ucsd.forward.query.logical.Project;
import edu.ucsd.forward.query.logical.Project.Item;
import edu.ucsd.forward.query.logical.Project.ProjectQualifier;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.Scan.FlattenSemantics;
import edu.ucsd.forward.query.logical.Select;
import edu.ucsd.forward.query.logical.SemiJoin;
import edu.ucsd.forward.query.logical.SendPlan;
import edu.ucsd.forward.query.logical.SetOperator;
import edu.ucsd.forward.query.logical.Sort;
import edu.ucsd.forward.query.logical.Subquery;
import edu.ucsd.forward.query.logical.ddl.CreateDataObject;
import edu.ucsd.forward.query.logical.ddl.DropDataObject;
import edu.ucsd.forward.query.logical.dml.Delete;
import edu.ucsd.forward.query.logical.dml.Insert;
import edu.ucsd.forward.query.logical.dml.Update;
import edu.ucsd.forward.query.logical.dml.Update.Assignment;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.term.AbsoluteVariable;
import edu.ucsd.forward.query.logical.term.Constant;
import edu.ucsd.forward.query.logical.term.ElementVariable;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.PositionVariable;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.physical.AbstractAssignImpl;
import edu.ucsd.forward.query.physical.AbstractSendPlanImpl;
import edu.ucsd.forward.query.physical.ApplyPlanImpl;
import edu.ucsd.forward.query.physical.AssignMemoryToMemoryImpl;
import edu.ucsd.forward.query.physical.CopyImpl;
import edu.ucsd.forward.query.physical.ExistsImpl;
import edu.ucsd.forward.query.physical.OperatorImpl;
import edu.ucsd.forward.query.physical.SendPlanImpl;
import edu.ucsd.forward.query.physical.ddl.CreateDataObjectImpl;
import edu.ucsd.forward.query.physical.dml.AbstractUpdateImpl.AssignmentImpl;
import edu.ucsd.forward.query.physical.dml.InsertImpl;
import edu.ucsd.forward.query.physical.dml.UpdateImpl;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.xml.AbstractXmlSerializer;
import edu.ucsd.forward.xml.XmlUtil;

import java.util.List;
import static edu.ucsd.forward.xml.XmlUtil.getOnlyChildElement;

/**
 * Utility class to serialize a query plan to an XML DOM element.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * @author Romain Vernoux
 * 
 */
public final class PlanXmlSerializer extends AbstractXmlSerializer
{
    @SuppressWarnings("unused")
    private static final Logger   log                            = Logger.getLogger(PlanXmlSerializer.class);
    
    protected static final String VIEWBLOCK_TREE_ELM             = "viewblock_tree";
    
    protected static final String VIEWBLOCK_ELM                  = "viewblock";
    
    protected static final String VIEWBLOCK_TYPE_ATTR            = "type";
    
    protected static final String VIEWBLOCK_ID_ATTR              = "id";
    
    protected static final String VIEWBLOCK_BASE_DS_ATTR         = "base_data_source";
    
    protected static final String VIEWBLOCK_BASE_NAME_ATTR       = "base_name";
    
    protected static final String VIEWBLOCK_OUTPUT_TYPE_ELM      = "output_type";
    
    protected static final String VIEWBLOCK_DIFF_NAME_ATTR       = "diff_name";
    
    protected static final String VIEWBLOCK_TYPE_COMMON          = "common";
    
    protected static final String VIEWBLOCK_TYPE_EVOLVING        = "evolving";
    
    protected static final String VIEWBLOCK_DIFF_SOURCE_BLOCK_ID = "source_block";
    
    protected static final String VIEWBLOCK_DIFF_QUERY_ELM       = "diff_query";
    
    protected static final String VIEWBLOCK_DIFF_QUERIES_ELM     = "diff_queries";
    
    protected static final String VIEWBLOCK_QUERY_TYPE_ATTR      = "type";
    
    protected static final String VIEWBLOCK_QUERY_TYPE_STRING    = "string";
    
    protected static final String VIEWBLOCK_QUERY_TYPE_LOGICAL   = "logical";
    
    protected static final String VIEWBLOCK_QUERY_TYPE_PHYSICAL  = "physical";
    
    protected static final String VIEWBLOCK_DIFFS_ELM            = "diff";
    
    protected static final String VIEWBLOCK_INPUT_PATTERN_ELM    = "input_modpat";
    
    protected static final String VIEWBLOCK_OUTPUT_PATTERN_ELM   = "output_modpat";
    
    protected static final String VIEWBLOCK_DIFF_TUPLETYPE       = "tuple_type";
    
    protected static final String VIEWBLOCK_QUERY_EXPRESSION     = "query_expression";
    
    public static final String    QUERY_PLAN_ELM                 = "query_plan";
    
    protected static final String WRAPPING_ATTR                  = "wrapping";
    
    protected static final String IMPL_ATTR                      = "impl";
    
    protected static final String NAME_ATTR                      = "name";
    
    protected static final String ALIAS_ATTR                     = "alias";
    
    protected static final String POSITION_ATTR                  = "position_var";
    
    protected static final String ATTR_NAME_ATTR                 = "attr_name";
    
    protected static final String VAR_NAME_ATTR                  = "var_name";
    
    protected static final String TYPE_ATTR                      = "type";
    
    protected static final String VARIATION_ATTR                 = "variation";
    
    protected static final String EXECUTION_ATTR                 = "execution_data_source";
    
    protected static final String ESTIMATE_ATTR                  = "cardinality_estimate";
    
    protected static final String SET_QUANTIFIER_ATTR            = "set_quantifier";
    
    protected static final String TARGET_TYPE_ATTR               = "target_type";
    
    protected static final String ARGS_ELM                       = "arguments";
    
    protected static final String STEPS_ATTR                     = "steps";
    
    protected static final String SCHEMA_OBJECT_ATTR             = "schema_object";
    
    protected static final String DATA_SOURCE_ATTR               = "data_source";
    
    protected static final String MODE_ATTR                      = "mode";
    
    protected static final String OPTION_ELM                     = "option";
    
    protected static final String INDEX_NAME_ATTR                = "index_name";
    
    protected static final String CONDITION_ELM                  = "condition";
    
    protected static final String SORT_SPEC_ATTR                 = "spec";
    
    protected static final String SORT_NULLS_ATTR                = "nulls";
    
    protected static final String OFFSET_ELM                     = "offset";
    
    protected static final String FETCH_ELM                      = "fetch";
    
    protected static final String TARGET_DATA_SOURCE_ATTR        = "target_data_source";
    
    protected static final String TARGET_SCHEMA_OBJECT_ATTR      = "target_schema_object";
    
    protected static final String TARGET_ELM                     = "target";
    
    protected static final String ITEM_ELM                       = "item";
    
    protected static final String ATTRS_ELM                      = "attributes";
    
    protected static final String ATTR_ELM                       = "attribute";
    
    protected static final String DELETE_VAR_ATTR                = "delete_var";
    
    protected static final String UPDATE_VAR_ATTR                = "update_var";
    
    protected static final String ASSIGNMENT_ELM                 = "assignment";
    
    protected static final String INSTANTIATION_ATTR             = "instantiation";
    
    protected static final String KEY_RANGE_ELM                  = "key_range";
    
    protected static final String LOWER_ELM                      = "lower";
    
    protected static final String UPPER_ELM                      = "upper";
    
    protected static final String IS_OPEN_ATTR                   = "open";
    
    protected static final String PATH_ELM                       = "path";
    
    protected static final String NULL_VALUE                     = "null";
    
    protected static final String NAVIGATES                      = "navigates";
    
    protected static final String ASSIGN_LIST_ELM                = "assign_list";
    
    protected static final String TARGET_ATTR                    = "target";
    
    protected static final String PARTITION_BY_ELM               = "partition_terms";
    
    protected static final String LIMIT_ATTR                     = "limit";
    
    protected static final String RANK_ALIAS_ATTR                = "rank_alias";
    
    protected static final String SELECT_ELEMENT                 = "select_element";
    
    protected static final String APPLYPLAN_EXEC_CONDITION       = "exec_condition";
    
    protected enum VariableType
    {
        ABSOLUTE, RELATIVE, ELEMENT, POSITION
    }
    
    private boolean m_is_print_param_type = true;
    
    private boolean m_is_under_param      = false;
    
    /**
     * Constructor.
     */
    public PlanXmlSerializer()
    {
    }
    
    /**
     * Constructor.
     * 
     * @param document
     *            the existing document.
     */
    public PlanXmlSerializer(Document document)
    {
        super(document);
    }
    
    /**
     * Sets whether to print type of parameter.
     * 
     * @param flag
     *            the flag to set
     */
    public void setPrintParameterType(boolean flag)
    {
        m_is_print_param_type = flag;
    }
    
    /**
     * Gets simple name of the given class without package part.
     * 
     * @param clazz
     *            the given class.
     * @return simple name of the given class without package part.
     */
    private String getSimpleName(Class<?> clazz)
    {
        String full = clazz.getName();
        int idx1 = full.lastIndexOf(".");
        int idx2 = full.lastIndexOf("$");
        int idx = idx1 > idx2 ? idx1 : idx2;
        return full.substring(idx + 1); // strip the package
    }
    
    /**
     * Converts a term into XML element.
     * 
     * @param term
     *            the term
     * @return the element node that represents the term in XML.
     */
    public Element serializeTerm(Term term)
    {
        assert (term != null);
        
        Element element;
        
        if (term instanceof Constant)
        {
            String tag_name = getSimpleName(term.getClass());
            element = getDomDocument().createElement(tag_name);
            Constant constant = (Constant) term;
            Value value = constant.getValue();
            if (value instanceof ScalarValue)
            {
                XmlUtil.createLeafElement(TypeEnum.getName(value.getTypeClass()), value.toString(), element);
            }
            else
            {
                assert (value instanceof NullValue);
                Element leaf = XmlUtil.createLeafElement(TypeEnum.getName(value.getTypeClass()), null, element);
                leaf.setAttribute(NULL_VALUE, "true");
            }
        }
        else if (term instanceof QueryPath)
        {
            String tag_name = getSimpleName(term.getClass());
            element = getDomDocument().createElement(tag_name);
            QueryPath path = (QueryPath) term;
            element.appendChild(serializeTerm(path.getTerm()));
            
            String steps = "";
            for (String step : path.getPathSteps())
            {
                steps += step + ".";
            }
            steps = steps.substring(0, steps.length() - 1);
            element.setAttribute(STEPS_ATTR, steps);
        }
        else if (term instanceof Parameter)
        {
            Parameter param = (Parameter) term;
            
            String tag_name = getSimpleName(term.getClass());
            element = getDomDocument().createElement(tag_name);
            
            switch (param.getInstantiationMethod())
            {
                case ASSIGN:
                    break;
                case COPY:
                    element.setAttribute(DATA_SOURCE_ATTR, param.getDataSourceName());
                    element.setAttribute(SCHEMA_OBJECT_ATTR, param.getSchemaObjectName());
                    break;
            }
            
            m_is_under_param = true;
            element.appendChild(serializeTerm(param.getTerm()));
            m_is_under_param = false;
        }
        else if (term instanceof AbsoluteVariable)
        {
            AbsoluteVariable variable = (AbsoluteVariable) term;
            
            String tag_name = getSimpleName(Variable.class);
            element = getDomDocument().createElement(tag_name);
            
            element.setAttribute(DATA_SOURCE_ATTR, variable.getDataSourceName());
            element.setAttribute(SCHEMA_OBJECT_ATTR, variable.getSchemaObjectName());
            element.setAttribute(MODE_ATTR, VariableType.ABSOLUTE.name());
        }
        else if (term instanceof RelativeVariable)
        {
            RelativeVariable variable = (RelativeVariable) term;
            
            String tag_name = getSimpleName(Variable.class);
            
            element = getDomDocument().createElement(tag_name);
            
            element.setAttribute(NAME_ATTR, variable.getName());
            
            if (term instanceof ElementVariable)
            {
                element.setAttribute(MODE_ATTR, VariableType.ELEMENT.name());
            }
            else if (term instanceof PositionVariable)
            {
                element.setAttribute(MODE_ATTR, VariableType.POSITION.name());
            }
            
            if (m_is_print_param_type && m_is_under_param)
            {
                // Add the variable type
                TypeXmlSerializer.serializeType(variable.getType(), TYPE_ATTR, element);
            }
        }
        else if (term instanceof FunctionCall)
        {
            FunctionCall<?> fc = (FunctionCall<?>) term;
            String tag_name = getSimpleName(FunctionCall.class);
            element = getDomDocument().createElement(tag_name);
            element.setAttribute(NAME_ATTR, fc.getFunction().getName());
            
            for (Term child_term : fc.getArguments())
            {
                element.appendChild(serializeTerm(child_term));
            }
            
            if (fc instanceof ExternalFunctionCall)
            {
                ExternalFunctionCall external_f_call = (ExternalFunctionCall) fc;
                element.setAttribute(TARGET_DATA_SOURCE_ATTR, external_f_call.getFunction().getTargetDataSource());
            }
            
            if (fc instanceof AggregateFunctionCall)
            {
                AggregateFunctionCall agg_f_call = (AggregateFunctionCall) fc;
                element.setAttribute(SET_QUANTIFIER_ATTR, agg_f_call.getSetQuantifier().name());
            }
            
            if (fc instanceof CastFunctionCall)
            {
                CastFunctionCall cast_f_call = (CastFunctionCall) fc;
                element.setAttribute(TARGET_TYPE_ATTR, cast_f_call.getTargetType().getName());
            }
        }
        else
        {
            throw new UnsupportedOperationException();
        }
        
        return element;
    }
    
    /**
     * Converts an aggregate into an XML element.
     * 
     * @param aggregate
     *            the aggregate
     * @return the converted XML element
     */
    private Element serializeAggregate(Aggregate aggregate)
    {
        Element aggregate_element = getDomDocument().createElement(getSimpleName(Aggregate.class));
        aggregate_element.setAttribute(ALIAS_ATTR, aggregate.getAlias());
        
        AggregateFunctionCall call = aggregate.getAggregateFunctionCall();
        String tag_name = getSimpleName(FunctionCall.class);
        Element call_element = getDomDocument().createElement(tag_name);
        call_element.setAttribute(NAME_ATTR, call.getFunction().getName());
        if (call.getSetQuantifier() != null)
        {
            call_element.setAttribute(SET_QUANTIFIER_ATTR, call.getSetQuantifier().name());
        }
        for (Term argument : call.getArguments())
        {
            call_element.appendChild(serializeTerm(argument));
        }
        aggregate_element.appendChild(call_element);
        
        return aggregate_element;
    }
    
    /**
     * Converts a logical operator into XML element, this method does not convert the child operators of the operator.
     * 
     * @param operator
     *            the logical operator
     * @return the element node that represents the logical operator in XML.
     */
    private Element serializeOperatorOnly(Operator operator)
    {
        // Operator XML element
        Element operator_elm = getDomDocument().createElement(operator.getName());
        
        // Arguments XML element
        Element args_elm = getDomDocument().createElement(ARGS_ELM);
        operator_elm.appendChild(args_elm);
        
        if (operator.getClass() == Assign.class)
        {
            Assign assign = (Assign) operator;
            operator_elm.setAttribute(TARGET_ATTR, assign.getTarget());
        }
        else if (operator.getClass() == IndexScan.class)
        {
            IndexScan index_scan = (IndexScan) operator;
            operator_elm.setAttribute(INDEX_NAME_ATTR, index_scan.getIndexName());
            
            // Set alias
            String alias = index_scan.getAliasVariable().getName();
            if (alias != null) operator_elm.setAttribute(ALIAS_ATTR, alias);
            
            // path element
            Element path_elm = getDomDocument().createElement(PATH_ELM);
            path_elm.appendChild(serializeTerm(index_scan.getTerm()));
            args_elm.appendChild(path_elm);
            // key range elements
            for (KeyRangeSpec spec : index_scan.getKeyRangeSpecs())
            {
                Element spec_elm = getDomDocument().createElement(KEY_RANGE_ELM);
                args_elm.appendChild(spec_elm);
                if (spec.getLowerTerm() != null)
                {
                    Element lower_elm = getDomDocument().createElement(LOWER_ELM);
                    lower_elm.appendChild(serializeTerm(spec.getLowerTerm()));
                    lower_elm.setAttribute(IS_OPEN_ATTR, "" + spec.isLowerOpen());
                    spec_elm.appendChild(lower_elm);
                }
                if (spec.getUpperTerm() != null)
                {
                    Element upper_elm = getDomDocument().createElement(UPPER_ELM);
                    upper_elm.appendChild(serializeTerm(spec.getUpperTerm()));
                    upper_elm.setAttribute(IS_OPEN_ATTR, "" + spec.isUpperOpen());
                    spec_elm.appendChild(upper_elm);
                }
            }
        }
        else if (operator.getClass() == Scan.class)
        {
            Scan scan = (Scan) operator;
            
            // Set alias
            String alias = scan.getAliasVariable().getName();
            if (alias != null) operator_elm.setAttribute(ALIAS_ATTR, alias);
            
            // Set position variable
            if (scan.getOrderVariable() != null)
            {
                String pos_var = scan.getOrderVariable().getName();
                operator_elm.setAttribute(POSITION_ATTR, pos_var);
            }
            
            if(scan.getFlattenSemantics() == FlattenSemantics.OUTER)
            {
                operator_elm.setAttribute(VARIATION_ATTR, FlattenSemantics.OUTER.toString());
            }
            
            // Set scan term
            args_elm.appendChild(serializeTerm(scan.getTerm()));
        }
        else if (operator.getClass() == Subquery.class)
        {
            Subquery subquery = (Subquery) operator;
            
            // Set alias
            String alias = subquery.getAliasVariable().getName();
            if (alias != null) operator_elm.setAttribute(ALIAS_ATTR, alias);
            
            // Set position variable
            if (subquery.getOrderVariable() != null)
            {
                String pos_var = subquery.getOrderVariable().getName();
                operator_elm.setAttribute(POSITION_ATTR, pos_var);
            }
        }
        else if (operator.getClass() == Navigate.class)
        {
            Navigate navigate = (Navigate) operator;
            
            // Set alias variable
            if (navigate.getAliasVariable() != null)
            {
                String alias = navigate.getAliasVariable().getName();
                if (alias != null) operator_elm.setAttribute(ALIAS_ATTR, alias);
            }
            
            // Set navigate term
            if (navigate.getTerm() != null)
            {
                args_elm.appendChild(serializeTerm(navigate.getTerm()));
            }
        }
        else if (operator.getClass() == Ground.class)
        {
            // Do nothing
        }
        else if (operator.getClass() == OuterJoin.class)
        {
            OuterJoin join = (OuterJoin) operator;
            // Add join conditions
            for (Term condition : join.getConditions())
            {
                args_elm.appendChild(serializeTerm(condition));
            }
            // Set variation
            operator_elm.setAttribute(VARIATION_ATTR, join.getOuterJoinVariation().toString());
        }
        else if (operator.getClass() == AntiSemiJoin.class)
        {
            AntiSemiJoin join = (AntiSemiJoin) operator;
            // Add join conditions
            for (Term condition : join.getConditions())
            {
                args_elm.appendChild(serializeTerm(condition));
            }
        }
        else if (operator.getClass() == SemiJoin.class)
        {
            SemiJoin join = (SemiJoin) operator;
            // Add join conditions
            for (Term condition : join.getConditions())
            {
                args_elm.appendChild(serializeTerm(condition));
            }
        }
        else if (operator.getClass() == InnerJoin.class)
        {
            InnerJoin join = (InnerJoin) operator;
            // Add join conditions
            for (Term condition : join.getConditions())
            {
                args_elm.appendChild(serializeTerm(condition));
            }
        }
        else if (operator.getClass() == Project.class)
        {
            Project project = (Project) operator;
            if (project.getProjectQualifier() == ProjectQualifier.ELEMENT)
            {
                operator_elm.setAttribute(SELECT_ELEMENT, String.valueOf(true));
            }
            for (Item item : project.getProjectionItems())
            {
                Element item_element = getDomDocument().createElement(getSimpleName(Project.Item.class));
                item_element.setAttribute(ALIAS_ATTR, item.getAlias());
                item_element.appendChild(serializeTerm(item.getTerm()));
                args_elm.appendChild(item_element);
            }
        }
        else if (operator.getClass() == Select.class)
        {
            Select select = (Select) operator;
            // Add selection conditions
            for (Term condition : select.getConditions())
            {
                args_elm.appendChild(serializeTerm(condition));
            }
        }
        else if (operator.getClass() == GroupBy.class)
        {
            GroupBy group_by = (GroupBy) operator;
            List<edu.ucsd.forward.query.logical.GroupBy.Item> group_by_items = group_by.getGroupByItems();
            for (edu.ucsd.forward.query.logical.GroupBy.Item item : group_by_items)
            {
                Element group_by_item_element = getDomDocument().createElement(getSimpleName(GroupByItem.class));
                group_by_item_element.appendChild(serializeTerm(item.getTerm()));
                group_by_item_element.setAttribute(ATTR_NAME_ATTR, item.getVariable().getName());
                args_elm.appendChild(group_by_item_element);
            }
            for (Aggregate aggregate : group_by.getAggregates())
            {
                args_elm.appendChild(serializeAggregate(aggregate));
            }
        }
        else if (operator.getClass() == PartitionBy.class)
        {
            PartitionBy partition_by = (PartitionBy) operator;
            // Set limit
            IntegerValue limit = partition_by.getLimit();
            if (limit != null) operator_elm.setAttribute(LIMIT_ATTR, limit.toString());
            // Set rank functions
            if (partition_by.hasRankFunction())
            {
                operator_elm.setAttribute(RANK_ALIAS_ATTR, partition_by.getRankAlias());
            }
            // Set partition terms
            List<RelativeVariable> partition_by_terms = partition_by.getPartitionByTerms();
            Element partitions_elm = getDomDocument().createElement(PARTITION_BY_ELM);
            for (RelativeVariable variable : partition_by_terms)
            {
                Element var_elm = serializeTerm(variable);
                partitions_elm.appendChild(var_elm);
            }
            args_elm.appendChild(partitions_elm);
            // Set order by terms
            List<Sort.Item> sort_by_terms = partition_by.getSortByItems();
            for (Sort.Item item : sort_by_terms)
            {
                Element item_element = getDomDocument().createElement(getSimpleName(Sort.Item.class));
                item_element.setAttribute(SORT_SPEC_ATTR, item.getSpec().name());
                item_element.setAttribute(SORT_NULLS_ATTR, item.getNulls().name());
                item_element.appendChild(serializeTerm(item.getTerm()));
                args_elm.appendChild(item_element);
            }
        }
        
        else if (operator.getClass() == SetOperator.class)
        {
            SetOperator set_op = (SetOperator) operator;
            operator_elm.setAttribute(SET_QUANTIFIER_ATTR, set_op.getSetQuantifier().name());
        }
        else if (operator.getClass() == Sort.class)
        {
            Sort sort = (Sort) operator;
            for (Sort.Item item : sort.getSortItems())
            {
                Element item_element = getDomDocument().createElement(getSimpleName(Sort.Item.class));
                item_element.setAttribute(SORT_SPEC_ATTR, item.getSpec().name());
                item_element.setAttribute(SORT_NULLS_ATTR, item.getNulls().name());
                item_element.appendChild(serializeTerm(item.getTerm()));
                args_elm.appendChild(item_element);
            }
        }
        else if (operator.getClass() == OffsetFetch.class)
        {
            OffsetFetch offset_fetch = (OffsetFetch) operator;
            if (offset_fetch.getOffset() != null)
            {
                Element offset_elm = getDomDocument().createElement(OFFSET_ELM);
                offset_elm.appendChild(serializeTerm(offset_fetch.getOffset()));
                args_elm.appendChild(offset_elm);
            }
            if (offset_fetch.getFetch() != null)
            {
                Element fetch_elm = getDomDocument().createElement(FETCH_ELM);
                fetch_elm.appendChild(serializeTerm(offset_fetch.getFetch()));
                args_elm.appendChild(fetch_elm);
            }
        }
        else if (operator.getClass() == SendPlan.class)
        {
            // Do nothing
        }
        else if (operator.getClass() == Copy.class)
        {
            // Set the target data source
            Copy copy = (Copy) operator;
            operator_elm.setAttribute(TARGET_DATA_SOURCE_ATTR, copy.getTargetDataSourceName());
            operator_elm.setAttribute(TARGET_SCHEMA_OBJECT_ATTR, copy.getTargetSchemaObjectName());
        }
        else if (operator.getClass() == ApplyPlan.class)
        {
            // Set the alias
            ApplyPlan apply_plan = (ApplyPlan) operator;
            operator_elm.setAttribute(ATTR_NAME_ATTR, apply_plan.getAlias());
            
            // Set the execution condition
            if (apply_plan.hasExecutionCondition())
            {
                Element cond_elm = getDomDocument().createElement(APPLYPLAN_EXEC_CONDITION);
                cond_elm.appendChild(serializeTerm(apply_plan.getExecutionCondition()));
                args_elm.appendChild(cond_elm);
            }
        }
        else if (operator.getClass() == Exists.class)
        {
            // Set the alias
            Exists exists_op = (Exists) operator;
            operator_elm.setAttribute(ATTR_NAME_ATTR, exists_op.getVariableName());
            
            // Set the execution condition
            if (exists_op.hasExecutionCondition())
            {
                Element cond_elm = getDomDocument().createElement(APPLYPLAN_EXEC_CONDITION);
                cond_elm.appendChild(serializeTerm(exists_op.getExecutionCondition()));
                args_elm.appendChild(cond_elm);
            }
        }
        else if (operator.getClass() == CreateDataObject.class)
        {
            CreateDataObject definition = (CreateDataObject) operator;
            operator_elm.setAttribute(SCHEMA_OBJECT_ATTR, definition.getSchemaName());
            operator_elm.setAttribute(DATA_SOURCE_ATTR, definition.getDataSourceName());
            TypeXmlSerializer.serializeSchemaTree(definition.getSchemaTree(), args_elm);
        }
        else if (operator.getClass() == DropDataObject.class)
        {
            DropDataObject drop = (DropDataObject) operator;
            operator_elm.setAttribute(SCHEMA_OBJECT_ATTR, drop.getSchemaName());
            operator_elm.setAttribute(DATA_SOURCE_ATTR, drop.getDataSourceName());
        }
        else if (operator.getClass() == Insert.class)
        {
            Insert insert_op = (Insert) operator;
            
            // Set the target data source
            operator_elm.setAttribute(TARGET_DATA_SOURCE_ATTR, insert_op.getTargetDataSourceName());
            
            // Set the target query path
            Element target_elm = getDomDocument().createElement(TARGET_ELM);
            args_elm.appendChild(target_elm);
            target_elm.appendChild(this.serializeTerm(insert_op.getTargetTerm()));
            
            // Add target attributes
            Element attrs_elm = getDomDocument().createElement(ATTRS_ELM);
            args_elm.appendChild(attrs_elm);
            for (String attr : insert_op.getTargetAttributes())
            {
                Element target_attr_elm = getDomDocument().createElement(ATTR_ELM);
                target_attr_elm.setAttribute(NAME_ATTR, attr);
                attrs_elm.appendChild(target_attr_elm);
            }
        }
        else if (operator.getClass() == Delete.class)
        {
            Delete delete_op = (Delete) operator;
            // Set the target data source
            operator_elm.setAttribute(TARGET_DATA_SOURCE_ATTR, delete_op.getTargetDataSourceName());
            // Set the delete var
            operator_elm.setAttribute(DELETE_VAR_ATTR, "" + delete_op.getDeleteVariable().getName());
            
            Element target_elm = getDomDocument().createElement(TARGET_ELM);
            args_elm.appendChild(target_elm);
            
            // Set the target query path
            target_elm.appendChild(this.serializeTerm(delete_op.getTargetTerm()));
        }
        else if (operator.getClass() == Update.class)
        {
            Update update_op = (Update) operator;
            // Set the target data source
            operator_elm.setAttribute(TARGET_DATA_SOURCE_ATTR, update_op.getTargetDataSourceName());
            // Set the update var
            if (update_op.getUpdateVariable() != null) operator_elm.setAttribute(UPDATE_VAR_ATTR, ""
                    + update_op.getUpdateVariable().getName());
            
            Element target_elm = getDomDocument().createElement(TARGET_ELM);
            args_elm.appendChild(target_elm);
            
            // Set the target query path
            target_elm.appendChild(this.serializeTerm(update_op.getTargetTerm()));
        }
        
        // Set execution data source
        String execution_source = operator.getExecutionDataSourceName();
        if (execution_source != null)
        {
            operator_elm.setAttribute(EXECUTION_ATTR, execution_source);
        }
        
        // Set cardinality estimate
        Size cardinality_estimate = operator.getCardinalityEstimate();
        assert (cardinality_estimate != null);
        operator_elm.setAttribute(ESTIMATE_ATTR, cardinality_estimate.name());
        
        return operator_elm;
    }
    
    /**
     * Converts a logical operator into XML element, this method recursively converts all the child operators of the operator.
     * 
     * @param operator
     *            the logical operator
     * @return the element node that represents the logical operator in XML.
     */
    public Element serializeOperator(Operator operator)
    {
        Element element = serializeOperatorOnly(operator);
        
        // Operators with nested plans
        if (operator instanceof SendPlan)
        {
            Element args_elm = getOnlyChildElement(element, ARGS_ELM);
            SendPlan send_plan = (SendPlan) operator;
            serializeAssignConfiguration(args_elm, send_plan.getSendPlan());
            args_elm.appendChild(serializeOperator(send_plan.getSendPlan().getRootOperator()));
        }
        else if (operator instanceof Copy)
        {
            Element args_elm = getOnlyChildElement(element, ARGS_ELM);
            Copy copy = (Copy) operator;
            serializeAssignConfiguration(args_elm, copy.getCopyPlan());
            args_elm.appendChild(serializeOperator(copy.getCopyPlan().getRootOperator()));
        }
        else if (operator instanceof ApplyPlan)
        {
            Element args_elm = getOnlyChildElement(element, ARGS_ELM);
            ApplyPlan apply_plan = (ApplyPlan) operator;
            serializeAssignConfiguration(args_elm, apply_plan.getLogicalPlansUsed().get(0));
            args_elm.appendChild(serializeOperator(apply_plan.getLogicalPlansUsed().get(0).getRootOperator()));
        }
        else if (operator instanceof Assign)
        {
            Element args_elm = getOnlyChildElement(element, ARGS_ELM);
            Assign assign = (Assign) operator;
            serializeAssignConfiguration(args_elm, assign.getPlan());
            args_elm.appendChild(serializeOperator(assign.getPlan().getRootOperator()));
        }
        else if (operator instanceof Exists)
        {
            Element args_elm = getOnlyChildElement(element, ARGS_ELM);
            Exists exists = (Exists) operator;
            serializeAssignConfiguration(args_elm, exists.getLogicalPlansUsed().get(0));
            args_elm.appendChild(serializeOperator(exists.getLogicalPlansUsed().get(0).getRootOperator()));
        }
        else if (operator instanceof CreateDataObject)
        {
            Element args_elm = getOnlyChildElement(element, ARGS_ELM);
            CreateDataObject create_op = (CreateDataObject) operator;
            for (SchemaPath path : create_op.getTypesToSetDefault())
            {
                // Query plan XML element
                Element plan_elm = serializeLogicalPlan(create_op.getDefaultPlan(path));
                plan_elm.setAttribute(TYPE_ATTR, path.getString());
                args_elm.appendChild(plan_elm);
            }
        }
        else if (operator instanceof Insert)
        {
            Insert insert_op = (Insert) operator;
            
            Element args_elm = getOnlyChildElement(element, ARGS_ELM);
            
            // Query plan XML element
            // FIXME: the plan cannot have assignment, can it?
            Element plan_elm = serializeLogicalPlan(insert_op.getLogicalPlan());
            args_elm.appendChild(plan_elm);
        }
        else if (operator instanceof Update)
        {
            Element args_elm = getOnlyChildElement(element, ARGS_ELM);
            Update update_op = (Update) operator;
            
            for (Assignment assignment : update_op.getAssignments())
            {
                // Assignment XML element
                Element assignment_elm = getDomDocument().createElement(ASSIGNMENT_ELM);
                args_elm.appendChild(assignment_elm);
                
                // Target XML element
                Element target_elm = getDomDocument().createElement(TARGET_ELM);
                assignment_elm.appendChild(target_elm);
                
                target_elm.appendChild(this.serializeTerm(assignment.getTerm()));
                
                // Query plan XML element
                // FIXME: the plan cannot have assignment, can it?
                Element plan_elm = serializeLogicalPlan(assignment.getLogicalPlan());
                assignment_elm.appendChild(plan_elm);
                
            }
        }
        
        for (Operator child_operator : operator.getChildren())
        {
            Element child_element = serializeOperator(child_operator);
            element.appendChild(child_element);
        }
        
        return element;
    }
    
    /**
     * Converts a logical plan into the XML document representation.
     * 
     * @param plan
     *            the logical plan
     * @return the root element of the converted XML document
     */
    public Element serializeLogicalPlan(LogicalPlan plan)
    {
        Element root = getDomDocument().createElement(QUERY_PLAN_ELM);
        
        serializeAssignConfiguration(root, plan);
        root.appendChild(serializeOperator(plan.getRootOperator()));
        
        // Set the wrapping attribute
        root.setAttribute(WRAPPING_ATTR, Boolean.toString(plan.isWrapping()));
        
        return root;
    }
    
    /**
     * Serialize the assign operators configured in the logical plan.
     * 
     * @param elm
     *            the element node that represents the list of the assign operators.
     * @param plan
     *            the logical plan.
     */
    private void serializeAssignConfiguration(Element elm, LogicalPlan plan)
    {
        List<Assign> assigns = plan.getAssigns();
        if (assigns.isEmpty()) return;
        Element assign_list_elm = getDomDocument().createElement(ASSIGN_LIST_ELM);
        for (Assign assign : assigns)
        {
            assign_list_elm.appendChild(serializeOperator(assign));
        }
        elm.appendChild(assign_list_elm);
    }
    
    /**
     * Serialize all the assign implementation configured in the physical plan.
     * 
     * @param elm
     *            the element node that represents the list of the assign operators.
     * @param plan
     *            the physical plan.
     */
    private void serializeAssignImplConfiguration(Element elm, PhysicalPlan plan)
    {
        List<AbstractAssignImpl> assign_impls = plan.getAssignImpls();
        if (assign_impls.isEmpty()) return;
        Element assign_list_elm = getDomDocument().createElement(ASSIGN_LIST_ELM);
        for (AbstractAssignImpl assign_impl : assign_impls)
        {
            assign_list_elm.appendChild(serializeOperatorImpl(assign_impl));
        }
        elm.appendChild(assign_list_elm);
    }
    
    /**
     * Converts a physical operator into XML element.
     * 
     * @param opim
     *            the operator implementation to be converted.
     * @return the element node that represents the physical operator in XML.
     */
    private Element serializeOperatorImpl(OperatorImpl opim)
    {
        Element element = serializeOperatorOnly(opim.getOperator());
        
        String impl_type = opim.getName();
        element.setAttribute(IMPL_ATTR, impl_type);
        
        if (opim instanceof SendPlanImpl)
        {
            Element args_elm = getOnlyChildElement(element, ARGS_ELM);
            AbstractSendPlanImpl send_plan_impl = (AbstractSendPlanImpl) opim;
            serializeAssignImplConfiguration(args_elm, send_plan_impl.getSendPlan());
            args_elm.appendChild(serializeOperatorImpl(send_plan_impl.getSendPlan().getRootOperatorImpl()));
        }
        else if (opim instanceof CopyImpl)
        {
            Element args_elm = getOnlyChildElement(element, ARGS_ELM);
            CopyImpl copy_impl = (CopyImpl) opim;
            args_elm.appendChild(serializeOperatorImpl(copy_impl.getCopyPlan().getRootOperatorImpl()));
        }
        else if (opim instanceof AssignMemoryToMemoryImpl)
        {
            Element args_elm = getOnlyChildElement(element, ARGS_ELM);
            AssignMemoryToMemoryImpl assign_impl = (AssignMemoryToMemoryImpl) opim;
            serializeAssignImplConfiguration(args_elm, assign_impl.getPhysicalPlansUsed().get(0));
            args_elm.appendChild(serializeOperatorImpl(assign_impl.getPhysicalPlansUsed().get(0).getRootOperatorImpl()));
        }
        else if (opim instanceof ApplyPlanImpl)
        {
            Element args_elm = getOnlyChildElement(element, ARGS_ELM);
            ApplyPlanImpl apply_plan_impl = (ApplyPlanImpl) opim;
            serializeAssignImplConfiguration(args_elm, apply_plan_impl.getPhysicalPlansUsed().get(0));
            args_elm.appendChild(serializeOperatorImpl(apply_plan_impl.getPhysicalPlansUsed().get(0).getRootOperatorImpl()));
        }
        else if (opim instanceof ExistsImpl)
        {
            Element args_elm = getOnlyChildElement(element, ARGS_ELM);
            ExistsImpl exists_impl = (ExistsImpl) opim;
            serializeAssignImplConfiguration(args_elm, exists_impl.getPhysicalPlansUsed().get(0));
            args_elm.appendChild(serializeOperatorImpl(exists_impl.getPhysicalPlansUsed().get(0).getRootOperatorImpl()));
        }
        else if (opim instanceof CreateDataObjectImpl)
        {
            Element args_elm = getOnlyChildElement(element, ARGS_ELM);
            CreateDataObjectImpl create_opim = (CreateDataObjectImpl) opim;
            for (SchemaPath path : create_opim.getTypesToSetDefault())
            {
                // Query plan XML element
                Element plan_elm = serializePhysicalPlan(create_opim.getDefaultPlan(path));
                plan_elm.setAttribute(TYPE_ATTR, path.getString());
                args_elm.appendChild(plan_elm);
            }
        }
        else if (opim instanceof InsertImpl)
        {
            Element args_elm = getOnlyChildElement(element, ARGS_ELM);
            InsertImpl insert_impl = (InsertImpl) opim;
            
            // Query plan XML element
            serializeAssignImplConfiguration(args_elm, insert_impl.getInsertPlan());
            Element plan_elm = serializePhysicalPlan(insert_impl.getInsertPlan());
            args_elm.appendChild(plan_elm);
            
        }
        else if (opim instanceof UpdateImpl)
        {
            Element args_elm = getOnlyChildElement(element, ARGS_ELM);
            UpdateImpl update_opim = (UpdateImpl) opim;
            
            for (AssignmentImpl assignment_impl : update_opim.getAssignmentImpls())
            {
                // Assignment XML element
                Element assignment_elm = getDomDocument().createElement(ASSIGNMENT_ELM);
                args_elm.appendChild(assignment_elm);
                
                // Target XML element
                Element target_elm = getDomDocument().createElement(TARGET_ELM);
                assignment_elm.appendChild(target_elm);
                
                target_elm.appendChild(this.serializeTerm(assignment_impl.getAssignment().getTerm()));
                
                // Query plan XML element
                serializeAssignImplConfiguration(args_elm, assignment_impl.getPhysicalPlan());
                Element plan_elm = serializePhysicalPlan(assignment_impl.getPhysicalPlan());
                assignment_elm.appendChild(plan_elm);
            }
        }
        
        for (OperatorImpl child_impl : opim.getChildren())
        {
            element.appendChild(serializeOperatorImpl(child_impl));
        }
        
        return element;
    }
    
    /**
     * Converts a physical plan into the XML document representation.
     * 
     * @param plan
     *            the physical plan
     * @return the root element of the converted XML document
     */
    public Element serializePhysicalPlan(PhysicalPlan plan)
    {
        Element root = getDomDocument().createElement(QUERY_PLAN_ELM);
        
        serializeAssignImplConfiguration(root, plan);
        root.appendChild(serializeOperatorImpl(plan.getRootOperatorImpl()));
        
        // Set the wrapping attribute
        root.setAttribute(WRAPPING_ATTR, Boolean.toString(plan.getLogicalPlan().isWrapping()));
        
        return root;
    }
}
