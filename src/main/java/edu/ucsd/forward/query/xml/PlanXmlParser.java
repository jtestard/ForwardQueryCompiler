/**
 * 
 */
package edu.ucsd.forward.query.xml;

import static edu.ucsd.app2you.util.StringUtil.format;
import static edu.ucsd.forward.xml.XmlUtil.getAttribute;
import static edu.ucsd.forward.xml.XmlUtil.getChildElements;
import static edu.ucsd.forward.xml.XmlUtil.getNonEmptyAttribute;
import static edu.ucsd.forward.xml.XmlUtil.getOnlyChildElement;
import static edu.ucsd.forward.xml.XmlUtil.getOptionalChildElement;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.StringUtil;
import edu.ucsd.app2you.util.SystemUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.NullType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.type.TypeException;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.data.xml.TypeXmlParser;
import edu.ucsd.forward.data.xml.TypeXmlSerializer;
import edu.ucsd.forward.data.xml.ValueXmlParser;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.GroupByItem;
import edu.ucsd.forward.query.ast.OrderByItem.Nulls;
import edu.ucsd.forward.query.ast.OrderByItem.Spec;
import edu.ucsd.forward.query.ast.SetOpExpression.SetOpType;
import edu.ucsd.forward.query.ast.SetQuantifier;
import edu.ucsd.forward.query.function.Function;
import edu.ucsd.forward.query.function.FunctionCall;
import edu.ucsd.forward.query.function.FunctionRegistry;
import edu.ucsd.forward.query.function.FunctionRegistryException;
import edu.ucsd.forward.query.function.aggregate.AggregateFunction;
import edu.ucsd.forward.query.function.aggregate.AggregateFunctionCall;
import edu.ucsd.forward.query.function.cast.CastFunction;
import edu.ucsd.forward.query.function.cast.CastFunctionCall;
import edu.ucsd.forward.query.function.collection.CollectionFunction;
import edu.ucsd.forward.query.function.collection.CollectionFunctionCall;
import edu.ucsd.forward.query.function.conditional.CaseFunction;
import edu.ucsd.forward.query.function.conditional.CaseFunctionCall;
import edu.ucsd.forward.query.function.external.ExternalFunction;
import edu.ucsd.forward.query.function.external.ExternalFunctionCall;
import edu.ucsd.forward.query.function.general.GeneralFunction;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.function.tuple.TupleFunction;
import edu.ucsd.forward.query.function.tuple.TupleFunctionCall;
import edu.ucsd.forward.query.logical.AntiSemiJoin;
import edu.ucsd.forward.query.logical.ApplyPlan;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;
import edu.ucsd.forward.query.logical.Copy;
import edu.ucsd.forward.query.logical.EliminateDuplicates;
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
import edu.ucsd.forward.query.logical.OuterJoin.Variation;
import edu.ucsd.forward.query.logical.Project.ProjectQualifier;
import edu.ucsd.forward.query.logical.PartitionBy;
import edu.ucsd.forward.query.logical.Placeholder;
import edu.ucsd.forward.query.logical.Product;
import edu.ucsd.forward.query.logical.Project;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.Select;
import edu.ucsd.forward.query.logical.SemiJoin;
import edu.ucsd.forward.query.logical.SendPlan;
import edu.ucsd.forward.query.logical.SetOperator;
import edu.ucsd.forward.query.logical.Sort;
import edu.ucsd.forward.query.logical.Subquery;
import edu.ucsd.forward.query.logical.Scan.FlattenSemantics;
import edu.ucsd.forward.query.logical.ddl.CreateDataObject;
import edu.ucsd.forward.query.logical.ddl.DropDataObject;
import edu.ucsd.forward.query.logical.dml.Delete;
import edu.ucsd.forward.query.logical.dml.Insert;
import edu.ucsd.forward.query.logical.dml.Update;
import edu.ucsd.forward.query.logical.dml.Update.Assignment;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.plan.LogicalPlanUtil;
import edu.ucsd.forward.query.logical.term.AbsoluteVariable;
import edu.ucsd.forward.query.logical.term.AbstractTerm;
import edu.ucsd.forward.query.logical.term.Constant;
import edu.ucsd.forward.query.logical.term.ElementVariable;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.PositionVariable;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.physical.AbstractAssignImpl;
import edu.ucsd.forward.query.physical.AbstractCopyImpl;
import edu.ucsd.forward.query.physical.AbstractSendPlanImpl;
import edu.ucsd.forward.query.physical.AntiSemiJoinImplNestedLoop;
import edu.ucsd.forward.query.physical.ApplyPlanImpl;
import edu.ucsd.forward.query.physical.AssignImplJDBC;
import edu.ucsd.forward.query.physical.AssignMemoryToMemoryImpl;
import edu.ucsd.forward.query.physical.CopyImplInMemory;
import edu.ucsd.forward.query.physical.EliminateDuplicatesImpl;
import edu.ucsd.forward.query.physical.ExceptImpl;
import edu.ucsd.forward.query.physical.ExistsImpl;
import edu.ucsd.forward.query.physical.GroundImpl;
import edu.ucsd.forward.query.physical.GroupByImpl;
import edu.ucsd.forward.query.physical.IndexScanImpl;
import edu.ucsd.forward.query.physical.InnerJoinImplNestedLoop;
import edu.ucsd.forward.query.physical.IntersectImpl;
import edu.ucsd.forward.query.physical.NavigateImpl;
import edu.ucsd.forward.query.physical.OffsetFetchImpl;
import edu.ucsd.forward.query.physical.OperatorImpl;
import edu.ucsd.forward.query.physical.OuterJoinImplNestedLoop;
import edu.ucsd.forward.query.physical.OuterUnionImpl;
import edu.ucsd.forward.query.physical.PartitionByImpl;
import edu.ucsd.forward.query.physical.ProductImplNestedLoop;
import edu.ucsd.forward.query.physical.ProjectImpl;
import edu.ucsd.forward.query.physical.ScanImpl;
import edu.ucsd.forward.query.physical.ScanImplReferencing;
import edu.ucsd.forward.query.physical.SelectImplSeq;
import edu.ucsd.forward.query.physical.SemiJoinImplNestedLoop;
import edu.ucsd.forward.query.physical.SendPlanImplIdb;
import edu.ucsd.forward.query.physical.SendPlanImplInMemory;
import edu.ucsd.forward.query.physical.SendPlanImplJdbc;
import edu.ucsd.forward.query.physical.SendPlanImplRemote;
import edu.ucsd.forward.query.physical.SortImpl;
import edu.ucsd.forward.query.physical.SubqueryImpl;
import edu.ucsd.forward.query.physical.UnionImpl;
import edu.ucsd.forward.query.physical.ddl.CreateDataObjectImpl;
import edu.ucsd.forward.query.physical.ddl.DropDataObjectImpl;
import edu.ucsd.forward.query.physical.dml.AbstractInsertImpl;
import edu.ucsd.forward.query.physical.dml.AbstractUpdateImpl;
import edu.ucsd.forward.query.physical.dml.DeleteImplIdb;
import edu.ucsd.forward.query.physical.dml.DeleteImplInMemory;
import edu.ucsd.forward.query.physical.dml.DeleteImplJdbc;
import edu.ucsd.forward.query.physical.dml.InsertImplIdb;
import edu.ucsd.forward.query.physical.dml.InsertImplInMemory;
import edu.ucsd.forward.query.physical.dml.InsertImplJdbc;
import edu.ucsd.forward.query.physical.dml.UpdateImplIdb;
import edu.ucsd.forward.query.physical.dml.UpdateImplInMemory;
import edu.ucsd.forward.query.physical.dml.UpdateImplJdbc;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.xml.PlanXmlSerializer.VariableType;
import edu.ucsd.forward.xml.AbstractXmlParser;
import edu.ucsd.forward.xml.DomApiProxy;
import edu.ucsd.forward.xml.XmlParserException;

/**
 * Utility class to parse a query plan from an XML DOM element.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * @author Romain Vernoux
 * 
 */
public final class PlanXmlParser extends AbstractXmlParser
{
    @SuppressWarnings("unused")
    private static final Logger             log = Logger.getLogger(PlanXmlParser.class);
    
    /**
     * The mapping from the target name to the assign operator implementation.
     */
    private Map<String, AbstractAssignImpl> m_target_assign_map;
    
    /**
     * Constructor.
     */
    public PlanXmlParser()
    {
        m_target_assign_map = new HashMap<String, AbstractAssignImpl>();
    }
    
    /**
     * Creates the physical operator implementation specified by the tag name of the element.
     * 
     * @param element
     *            the element.
     * @param operator
     *            logical operator counter part.
     * 
     * @return the created physical operator implementation specified by the tag name of the element; returns <code>null</code> if
     *         the element does not specify an operator implementation.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseOperatorImpl(Element element, Operator operator) throws XmlParserException
    {
        assert operator != null;
        OperatorImpl opim = null;
        
        if (operator.getClass() == IndexScan.class)
        {
            opim = parseIndexScanImpl(element, operator);
        }
        else if (operator.getClass() == Scan.class)
        {
            opim = parseScanImpl(element, operator);
        }
        else if (operator.getClass() == Navigate.class)
        {
            opim = parseNavigateImpl(element, operator);
        }
        else if (operator.getClass() == AntiSemiJoin.class)
        {
            opim = parseAntiSemiJoinImpl(element, operator);
        }
        else if (operator.getClass() == ApplyPlan.class)
        {
            opim = parseApplyPlanImpl(element, (ApplyPlan) operator);
        }
        else if (operator.getClass() == Assign.class)
        {
            opim = parseAssignImpl(element, (Assign) operator);
        }
        else if (operator.getClass() == Copy.class)
        {
            opim = parseCopyImpl(element, (Copy) operator);
        }
        else if (operator.getClass() == EliminateDuplicates.class)
        {
            opim = parseEliminateDuplicatesImpl(element, (EliminateDuplicates) operator);
        }
        else if (operator.getClass() == Exists.class)
        {
            opim = parseExistsImpl(element, (Exists) operator);
        }
        else if (operator.getClass() == Ground.class)
        {
            opim = parseGroundImpl(element, operator);
        }
        else if (operator.getClass() == GroupBy.class)
        {
            opim = parseGroupByImpl(element, (GroupBy) operator);
        }
        else if (operator.getClass() == PartitionBy.class)
        {
            opim = parsePartitionByImpl(element, (PartitionBy) operator);
        }
        else if (operator.getClass() == InnerJoin.class)
        {
            opim = parseInnerJoinImpl(element, operator);
        }
        else if (operator.getClass() == OffsetFetch.class)
        {
            opim = parseOffsetFetchImpl(element, (OffsetFetch) operator);
        }
        else if (operator.getClass() == OuterJoin.class)
        {
            opim = parseOuterJoinImpl(element, (OuterJoin) operator);
        }
        else if (operator.getClass() == Product.class)
        {
            opim = parseProductImpl(element, operator);
        }
        else if (operator.getClass() == Project.class)
        {
            opim = parseProjectImpl(element, operator);
        }
        else if (operator.getClass() == Select.class)
        {
            opim = parseSelectImpl(element, operator);
        }
        else if (operator.getClass() == SemiJoin.class)
        {
            opim = parseSemiJoinImpl(element, operator);
        }
        else if (operator.getClass() == SendPlan.class)
        {
            opim = parseSendPlanImpl(element, (SendPlan) operator);
        }
        else if (operator.getClass() == SetOperator.class)
        {
            opim = parseSetOpImpl(element, (SetOperator) operator);
        }
        else if (operator.getClass() == Sort.class)
        {
            opim = parseSortImpl(element, (Sort) operator);
        }
        else if (operator.getClass() == CreateDataObject.class)
        {
            opim = parseCreateDataObjectImpl(element, (CreateDataObject) operator);
        }
        else if (operator.getClass() == DropDataObject.class)
        {
            opim = parseDropSchemaObjectImpl(element, (DropDataObject) operator);
        }
        else if (operator.getClass() == Insert.class)
        {
            opim = parseInsertImpl(element, (Insert) operator);
        }
        else if (operator.getClass() == Delete.class)
        {
            opim = parseDeleteImpl(element, (Delete) operator);
        }
        else if (operator.getClass() == Update.class)
        {
            opim = parseUpdateImpl(element, (Update) operator);
        }
        else if (operator.getClass() == Subquery.class)
        {
            opim = parseSubqueryImpl(element, (Subquery) operator);
        }
        else
        {
            throw new UnsupportedOperationException();
        }
        
        return opim;
    }
    
    /**
     * Creates the update implementation.
     * 
     * @param element
     *            the element
     * @param operator
     *            the update operator.
     * @return the created update implementation.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseUpdateImpl(Element element, Update operator) throws XmlParserException
    {
        OperatorImpl child_opim = getChildOperatorImpl(element, operator.getChild());
        
        String impl_type = element.getAttribute(PlanXmlSerializer.IMPL_ATTR);
        AbstractUpdateImpl op_impl = null;
        if (compareName(impl_type, UpdateImplInMemory.class))
        {
            op_impl = new UpdateImplInMemory(operator, child_opim);
        }
        else if (compareName(impl_type, UpdateImplJdbc.class))
        {
            op_impl = new UpdateImplJdbc(operator, child_opim);
        }
        else if (compareName(impl_type, UpdateImplIdb.class))
        {
            op_impl = new UpdateImplIdb(operator, child_opim);
        }
        else
        {
            throw new IllegalArgumentException();
        }
        
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        
        List<Element> assignment_elms = getChildElements(args_elm, PlanXmlSerializer.ASSIGNMENT_ELM);
        int index = 0;
        for (Element assignment_elm : assignment_elms)
        {
            Assignment assignment = operator.getAssignments().get(index++);
            
            Element plan_elm = getOnlyChildElement(assignment_elm, PlanXmlSerializer.QUERY_PLAN_ELM);
            
            PhysicalPlan plan_impl = parsePhysicalPlan(plan_elm, assignment.getLogicalPlan());
            
            op_impl.addAssignmentImpl(op_impl.new AssignmentImpl(assignment, plan_impl));
        }
        
        return op_impl;
    }
    
    /**
     * Creates the delete implementation.
     * 
     * @param element
     *            the element
     * @param operator
     *            the delete operator
     * @return the created delete implementation.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseDeleteImpl(Element element, Delete operator) throws XmlParserException
    {
        OperatorImpl child_opim = getChildOperatorImpl(element, operator.getChild());
        
        String impl_type = element.getAttribute(PlanXmlSerializer.IMPL_ATTR);
        if (compareName(impl_type, DeleteImplInMemory.class))
        {
            return new DeleteImplInMemory(operator, child_opim);
        }
        else if (compareName(impl_type, DeleteImplJdbc.class))
        {
            return new DeleteImplJdbc(operator, child_opim);
        }
        else if (compareName(impl_type, DeleteImplIdb.class))
        {
            return new DeleteImplIdb(operator, child_opim);
        }
        else
        {
            throw new IllegalArgumentException();
        }
    }
    
    /**
     * Creates the insert implementation.
     * 
     * @param element
     *            the element
     * @param operator
     *            the insert operator
     * @return the created insert implementation.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseInsertImpl(Element element, Insert operator) throws XmlParserException
    {
        OperatorImpl child_opim = getChildOperatorImpl(element, operator.getChild());
        
        String impl_type = element.getAttribute(PlanXmlSerializer.IMPL_ATTR);
        AbstractInsertImpl op_impl = null;
        if (compareName(impl_type, InsertImplInMemory.class))
        {
            op_impl = new InsertImplInMemory(operator, child_opim);
        }
        else if (compareName(impl_type, InsertImplJdbc.class))
        {
            op_impl = new InsertImplJdbc(operator, child_opim);
        }
        else if (compareName(impl_type, InsertImplIdb.class))
        {
            op_impl = new InsertImplIdb(operator, child_opim);
        }
        else
        {
            throw new IllegalArgumentException();
        }
        
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        
        Element plan_elm = getOnlyChildElement(args_elm, PlanXmlSerializer.QUERY_PLAN_ELM);
        
        PhysicalPlan plan_impl = parsePhysicalPlan(plan_elm, op_impl.getOperator().getLogicalPlan());
        
        op_impl.setInsertPlan(plan_impl);
        
        return op_impl;
    }
    
    /**
     * Creates the drop schema object definition implementation.
     * 
     * @param element
     *            the element
     * @param operator
     *            the drop schema object operator
     * @return the schema object definition implementation.
     */
    private OperatorImpl parseDropSchemaObjectImpl(Element element, DropDataObject operator)
    {
        return new DropDataObjectImpl(operator);
    }
    
    /**
     * Creates the schema object definition implementation.
     * 
     * @param element
     *            the element
     * @param operator
     *            the schema object definition operator
     * @return the schema object definition implementation.
     * @throws XmlParserException
     *             if any things wrong happens
     */
    private OperatorImpl parseCreateDataObjectImpl(Element element, CreateDataObject operator) throws XmlParserException
    {
        CreateDataObjectImpl opim = new CreateDataObjectImpl(operator);
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        
        List<Element> default_plan_elms = getChildElements(args_elm, PlanXmlSerializer.QUERY_PLAN_ELM);
        for (Element default_plan_elm : default_plan_elms)
        {
            String path_str = default_plan_elm.getAttribute(PlanXmlSerializer.TYPE_ATTR);
            SchemaPath path;
            try
            {
                path = new SchemaPath(path_str);
            }
            catch (ParseException e)
            {
                throw new XmlParserException("Error parsing schema path", e);
            }
            LogicalPlan logical_plan = operator.getDefaultPlan(path);
            OperatorImpl default_plan_opim = getChildOperatorImpl(default_plan_elm, logical_plan.getRootOperator());
            PhysicalPlan plan_impl = new PhysicalPlan(default_plan_opim, logical_plan);
            
            opim.addDefaultPlan(path, plan_impl);
        }
        return opim;
        
    }
    
    /**
     * Creates the offset fetch implementation.
     * 
     * @param element
     *            the element
     * @param operator
     *            the offset fetch operator.
     * @return the created offset fetch implementation
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseOffsetFetchImpl(Element element, OffsetFetch operator) throws XmlParserException
    {
        OperatorImpl child_opim = getChildOperatorImpl(element, operator.getChild());
        return new OffsetFetchImpl(operator, child_opim);
    }
    
    /**
     * Creates the sort operator implementation.
     * 
     * @param element
     *            the element
     * @param operator
     *            the sort operator.
     * @return the created sort operator implementation.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseSortImpl(Element element, Sort operator) throws XmlParserException
    {
        OperatorImpl child_opim = getChildOperatorImpl(element, operator.getChild());
        return new SortImpl(operator, child_opim);
    }
    
    /**
     * Creates the set operator implementation specified by the element.
     * 
     * @param element
     *            the element
     * @param operator
     *            the logical operator of set operation
     * @return the created set operator implementation
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseSetOpImpl(Element element, SetOperator operator) throws XmlParserException
    {
        List<OperatorImpl> child_opims = getTwoChildOperatorImpl(element, operator);
        
        switch (operator.getSetOpType())
        {
            case UNION:
                return new UnionImpl(operator, child_opims.get(0), child_opims.get(1));
            case INTERSECT:
                return new IntersectImpl(operator, child_opims.get(0), child_opims.get(1));
            case EXCEPT:
                return new ExceptImpl(operator, child_opims.get(0), child_opims.get(1));
            case OUTER_UNION:
                return new OuterUnionImpl(operator, child_opims.get(0), child_opims.get(1));
            default:
                throw new UnsupportedOperationException();
        }
    }
    
    /**
     * Creates the group by operator implementation specified by the element.
     * 
     * @param element
     *            the element
     * @param operator
     *            the logical operator of group by
     * @return the created group by operator implementation
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseGroupByImpl(Element element, GroupBy operator) throws XmlParserException
    {
        OperatorImpl child_opim = getChildOperatorImpl(element, operator.getChild());
        return new GroupByImpl(operator, child_opim);
    }
    
    /**
     * Creates the outer join operator implementation specified by the element.
     * 
     * @param element
     *            the element
     * @param operator
     *            the logical operator of outer join
     * @return the created outer join operator implementation
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseOuterJoinImpl(Element element, OuterJoin operator) throws XmlParserException
    {
        String impl_type = element.getAttribute(PlanXmlSerializer.IMPL_ATTR);
        List<OperatorImpl> child_opims = getTwoChildOperatorImpl(element, operator);
        if (compareName(impl_type, OuterJoinImplNestedLoop.class))
        {
            return new OuterJoinImplNestedLoop(operator, child_opims.get(0), child_opims.get(1));
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }
    
    /**
     * Creates the send-plan operator implementation specified by the element.
     * 
     * @param element
     *            the element
     * @param operator
     *            the logical operator of send plan
     * @return the created send-plan operator implementation
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseSendPlanImpl(Element element, SendPlan operator) throws XmlParserException
    {
        String impl_type = element.getAttribute(PlanXmlSerializer.IMPL_ATTR);
        
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        
        PhysicalPlan plan = parsePhysicalPlan(args_elm, operator.getSendPlan());
        AbstractSendPlanImpl op_impl = null;
        if (compareName(impl_type, SendPlanImplInMemory.class))
        {
            op_impl = new SendPlanImplInMemory(operator);
        }
        else if (compareName(impl_type, SendPlanImplIdb.class))
        {
            op_impl = new SendPlanImplIdb(operator);
        }
        else if (compareName(impl_type, SendPlanImplRemote.class))
        {
            op_impl = new SendPlanImplRemote(operator);
        }
        else
        {
            assert compareName(impl_type, SendPlanImplJdbc.class);
            op_impl = new SendPlanImplJdbc(operator);
        }
        op_impl.setSendPlan(plan);
        // Add copy operator implementations
        int child_index = 0;
        for (Element child_element : getChildElements(element))
        {
            if (child_element.getTagName().equals(PlanXmlSerializer.ARGS_ELM)) continue;
            
            // FIXME The SendPlan operator is supposed to have many Copy operators and at most one non-copy operator
            // assert (compareName(child_element.getTagName(), Copy.class));
            
            Operator child = operator.getChildren().get(child_index++);
            
            OperatorImpl opim = parseOperatorImpl(child_element, child);
            op_impl.addChild(opim);
        }
        
        return op_impl;
    }
    
    /**
     * Creates the copy operator implementation specified by the element.
     * 
     * @param element
     *            the element.
     * @param operator
     *            the logical operator of copy.
     * @return the created copy operator implementation.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseCopyImpl(Element element, Copy operator) throws XmlParserException
    {
        String impl_type = element.getAttribute(PlanXmlSerializer.IMPL_ATTR);
        
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        
        PhysicalPlan plan = parsePhysicalPlan(args_elm, operator.getCopyPlan());
        if (compareName(impl_type, CopyImplInMemory.class))
        {
            AbstractCopyImpl op_impl = new CopyImplInMemory(operator);
            op_impl.setCopyPlan(plan);
            
            return op_impl;
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }
    
    /**
     * Creates the apply-plan operator implementation specified by the element.
     * 
     * @param element
     *            the element
     * @param operator
     *            the logical operator of apply plan
     * @return the created apply plan operator implementation
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseApplyPlanImpl(Element element, ApplyPlan operator) throws XmlParserException
    {
        String impl_type = element.getAttribute(PlanXmlSerializer.IMPL_ATTR);
        assert (compareName(impl_type, ApplyPlanImpl.class));
        
        OperatorImpl child_opim = getChildOperatorImpl(element, operator.getChild());
        ApplyPlanImpl op_impl = new ApplyPlanImpl(operator, child_opim);
        
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        
        PhysicalPlan plan = parsePhysicalPlan(args_elm, operator.getApplyPlan());
        op_impl.setApplyPlan(plan);
        
        return op_impl;
    }
    
    /**
     * Creates the assign operator implementation specified by the element.
     * 
     * @param element
     *            the element
     * @param operator
     *            the logical operator of assign
     * @return the created assign operator implementation
     * @throws XmlParserException
     *             when the data source is not accessible
     */
    private OperatorImpl parseAssignImpl(Element element, Assign operator) throws XmlParserException
    {
        AbstractAssignImpl op_impl = null;
        String impl_type = element.getAttribute(PlanXmlSerializer.IMPL_ATTR);
        if (compareName(impl_type, AssignMemoryToMemoryImpl.class))
        {
            op_impl = new AssignMemoryToMemoryImpl(operator);
        }
        else if (compareName(impl_type, AssignImplJDBC.class))
        {
            op_impl = new AssignImplJDBC(operator);
        }
        
        assert (op_impl != null);
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        
        PhysicalPlan plan = parsePhysicalPlan(args_elm, operator.getPlan());
        op_impl.setAssignPlan(plan);
        
        // Book keep the operator implementation
        if (m_target_assign_map.containsKey(operator.getTarget()))
        {
            throw new XmlParserException("The assign target " + operator.getTarget() + " is defined more than once.");
        }
        m_target_assign_map.put(operator.getTarget(), op_impl);
        
        return op_impl;
    }
    
    /**
     * Creates the exists operator implementation specified by the element.
     * 
     * @param element
     *            the element
     * @param operator
     *            the logical operator of exists
     * @return the created exists operator implementation
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseExistsImpl(Element element, Exists operator) throws XmlParserException
    {
        String impl_type = element.getAttribute(PlanXmlSerializer.IMPL_ATTR);
        assert (compareName(impl_type, ExistsImpl.class));
        
        OperatorImpl child_opim = getChildOperatorImpl(element, operator.getChild());
        ExistsImpl op_impl = new ExistsImpl(operator, child_opim);
        
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        
        PhysicalPlan plan = parsePhysicalPlan(args_elm, operator.getLogicalPlansUsed().get(0));
        op_impl.setExistsPlan(plan);
        
        return op_impl;
    }
    
    /**
     * Creates the eliminate duplicates operator implementation specified by the element.
     * 
     * @param element
     *            the element
     * @param operator
     *            the logical operator
     * @return the created eliminate-duplicates operator implementation
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseEliminateDuplicatesImpl(Element element, EliminateDuplicates operator) throws XmlParserException
    {
        OperatorImpl child_opim = getChildOperatorImpl(element, operator.getChild());
        return new EliminateDuplicatesImpl(operator, child_opim);
    }
    
    /**
     * Creates the select implementation specified by the element.
     * 
     * @param element
     *            the element
     * @param operator
     *            the operator
     * @return the created select implementation
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseSelectImpl(Element element, Operator operator) throws XmlParserException
    {
        String impl_type = element.getAttribute(PlanXmlSerializer.IMPL_ATTR);
        Select select = (Select) operator;
        OperatorImpl child_opim = getChildOperatorImpl(element, select.getChild());
        if (compareName(impl_type, SelectImplSeq.class))
        {
            return new SelectImplSeq(select, child_opim);
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }
    
    /**
     * Creates the project implementation specified by the element.
     * 
     * @param element
     *            the element
     * @param operator
     *            the operator
     * @return the created project implementation
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseProjectImpl(Element element, Operator operator) throws XmlParserException
    {
        Project project = (Project) operator;
        OperatorImpl child_opim = getChildOperatorImpl(element, project.getChildren().get(0));
        return new ProjectImpl((Project) operator, child_opim);
    }
    
    /**
     * Creates the product implementation specified by the element.
     * 
     * @param element
     *            the element
     * @param operator
     *            the operator
     * @return the created product implementation
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseProductImpl(Element element, Operator operator) throws XmlParserException
    {
        String impl_type = element.getAttribute(PlanXmlSerializer.IMPL_ATTR);
        Product product = (Product) operator;
        List<OperatorImpl> child_opims = getTwoChildOperatorImpl(element, product);
        if (compareName(impl_type, ProductImplNestedLoop.class))
        {
            return new ProductImplNestedLoop(product, child_opims.get(0), child_opims.get(1));
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }
    
    /**
     * Creates the join implementation specified by the element.
     * 
     * @param element
     *            the element
     * @param operator
     *            the operator
     * @return the created join implementation
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseInnerJoinImpl(Element element, Operator operator) throws XmlParserException
    {
        String impl_type = element.getAttribute(PlanXmlSerializer.IMPL_ATTR);
        InnerJoin join = (InnerJoin) operator;
        List<OperatorImpl> child_opims = getTwoChildOperatorImpl(element, join);
        if (compareName(impl_type, InnerJoinImplNestedLoop.class))
        {
            return new InnerJoinImplNestedLoop(join, child_opims.get(0), child_opims.get(1));
        }
        else
        {
            String s = StringUtil.format("Operator implementation is not supported: %s", impl_type);
            throw new XmlParserException(s);
        }
    }
    
    /**
     * Creates the semi-join implementation specified by the element.
     * 
     * @param element
     *            the element
     * @param operator
     *            the operator
     * @return the created semi-join implementation
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseSemiJoinImpl(Element element, Operator operator) throws XmlParserException
    {
        String impl_type = element.getAttribute(PlanXmlSerializer.IMPL_ATTR);
        SemiJoin join = (SemiJoin) operator;
        List<OperatorImpl> child_opims = getTwoChildOperatorImpl(element, join);
        if (compareName(impl_type, SemiJoinImplNestedLoop.class))
        {
            return new SemiJoinImplNestedLoop(join, child_opims.get(0), child_opims.get(1));
        }
        else
        {
            String s = StringUtil.format("Operator implementation is not supported: %s", impl_type);
            throw new XmlParserException(s);
        }
    }
    
    /**
     * Creates the anti-semi-join implementation specified by the element.
     * 
     * @param element
     *            the element
     * @param operator
     *            the operator
     * @return the created anti-semi-join implementation
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseAntiSemiJoinImpl(Element element, Operator operator) throws XmlParserException
    {
        String impl_type = element.getAttribute(PlanXmlSerializer.IMPL_ATTR);
        AntiSemiJoin join = (AntiSemiJoin) operator;
        List<OperatorImpl> child_opims = getTwoChildOperatorImpl(element, join);
        if (compareName(impl_type, AntiSemiJoinImplNestedLoop.class))
        {
            return new AntiSemiJoinImplNestedLoop(join, child_opims.get(0), child_opims.get(1));
        }
        else
        {
            String s = StringUtil.format("Operator implementation is not supported: %s", impl_type);
            throw new XmlParserException(s);
        }
    }
    
    /**
     * Creates the index scan implementation specified by the element.
     * 
     * @param element
     *            the element
     * @param operator
     *            the operator
     * @return the created index scan implementation
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseIndexScanImpl(Element element, Operator operator) throws XmlParserException
    {
        OperatorImpl child_opim = getChildOperatorImpl(element, operator.getChildren().get(0));
        return new IndexScanImpl((IndexScan) operator, child_opim);
    }
    
    /**
     * Creates the scan implementation specified by the element.
     * 
     * @param element
     *            the element
     * @param operator
     *            the operator
     * @return the created scan implementation
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseScanImpl(Element element, Operator operator) throws XmlParserException
    {
        String impl_type = element.getAttribute(PlanXmlSerializer.IMPL_ATTR);
        OperatorImpl child_opim = getChildOperatorImpl(element, operator.getChildren().get(0));
        Scan scan = (Scan) operator;
        if (compareName(impl_type, ScanImplReferencing.class))
        {
            ScanImplReferencing opim = new ScanImplReferencing(scan, child_opim);
            // Set the referenced assign impl
            Term term = scan.getTerm();
            if (!(term instanceof AbsoluteVariable)) throw new XmlParserException(
                                                                                  "The reference scan operator should be configured with absolute variable "
                                                                                          + term.toExplainString());
            String target = ((AbsoluteVariable) term).getSchemaObjectName();
            if (!m_target_assign_map.containsKey(target)) throw new XmlParserException("The reference " + target
                    + " is not defined by assign operator");
            opim.setReferencedAssign(m_target_assign_map.get(target));
            return opim;
        }
        return new ScanImpl((Scan) operator, child_opim);
    }
    
    /**
     * Creates the navigate implementation specified by the element.
     * 
     * @param element
     *            the element
     * @param operator
     *            the operator
     * @return the created navigate implementation
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parseNavigateImpl(Element element, Operator operator) throws XmlParserException
    {
        OperatorImpl child_opim = getChildOperatorImpl(element, operator.getChildren().get(0));
        return new NavigateImpl((Navigate) operator, child_opim);
    }
    
    /**
     * Creates the data object ground implementation specified by the element.
     * 
     * @param element
     *            the element
     * @param operator
     *            the operator
     * @return the created data object ground implementation
     */
    private OperatorImpl parseGroundImpl(Element element, Operator operator)
    {
        return new GroundImpl((Ground) operator);
    }
    
    /**
     * Gets the child operator implementation for an operator's child.
     * 
     * @param element
     *            the element specifying the parent operator.
     * @param child_operator
     *            the child operator.
     * @return the child operator implementation for the parent operator's child.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl getChildOperatorImpl(Element element, Operator child_operator) throws XmlParserException
    {
        for (Element child_element : getChildElements(element))
        {
            if (!child_element.getTagName().equalsIgnoreCase(PlanXmlSerializer.ARGS_ELM)
                    && !child_element.getTagName().equalsIgnoreCase(PlanXmlSerializer.ASSIGN_LIST_ELM))
            {
                return parseOperatorImpl(child_element, child_operator);
            }
        }
        throw new UnsupportedOperationException();
    }
    
    /**
     * Gets the child operators implementation for the binary operator's children.
     * 
     * @param element
     *            the element specifying the binary operator.
     * @param operator
     *            the binary operator
     * @return the child operators implementation for the binary operator's children.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private List<OperatorImpl> getTwoChildOperatorImpl(Element element, Operator operator) throws XmlParserException
    {
        List<OperatorImpl> result = new ArrayList<OperatorImpl>();
        assert operator.getChildren().size() == 2;
        
        List<Operator> child_ops = new ArrayList<Operator>();
        child_ops.add(operator.getChildren().get(0));
        child_ops.add(operator.getChildren().get(1));
        
        for (Element child_element : getChildElements(element))
        {
            if (child_element.getTagName().equalsIgnoreCase(PlanXmlSerializer.ARGS_ELM)) continue;
            
            assert (!child_ops.isEmpty());
            result.add(parseOperatorImpl(child_element, child_ops.remove(0)));
        }
        assert (child_ops.isEmpty());
        assert result.size() == 2;
        
        return result;
    }
    
    /**
     * Creates the logical operator specified by the tag name of the element.
     * 
     * @param element
     *            the element node in XML
     * @return the created logical operator specified by the tag name of the element; returns <code>null</code> if the element does
     *         not specify an operator.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    public Operator parseOperator(Element element) throws XmlParserException
    {
        String name = element.getTagName();
        Operator operator = null;
        
        if (compareName(name, IndexScan.class))
        {
            operator = parseIndexScan(element);
        }
        else if (compareName(name, Scan.class))
        {
            operator = parseScan(element);
        }
        else if (compareName(name, Navigate.class))
        {
            operator = parseNavigate(element);
        }
        else if (compareName(name, Subquery.class))
        {
            operator = parseSubquery(element);
        }
        else if (compareName(name, AntiSemiJoin.class))
        {
            operator = parseAntiSemiJoin(element);
        }
        else if (compareName(name, Assign.class))
        {
            operator = parseAssign(element);
        }
        else if (compareName(name, ApplyPlan.class))
        {
            operator = parseApplyPlan(element);
        }
        else if (compareName(name, Copy.class))
        {
            operator = parseCopy(element);
        }
        else if (compareName(name, EliminateDuplicates.class))
        {
            operator = parseEliminateDuplicates(element);
        }
        else if (compareName(name, Exists.class))
        {
            operator = parseExists(element);
        }
        else if (compareName(name, Ground.class))
        {
            operator = parseGround(element);
        }
        else if (compareName(name, GroupBy.class))
        {
            operator = parseGroupBy(element);
        }
        else if (compareName(name, PartitionBy.class))
        {
            operator = parsePartitionBy(element);
        }
        else if (compareName(name, InnerJoin.class))
        {
            operator = parseInnerJoin(element);
        }
        else if (compareName(name, OffsetFetch.class))
        {
            operator = parseOffsetFetch(element);
        }
        else if (compareName(name, OuterJoin.class))
        {
            operator = parseOuterJoin(element);
        }
        else if (compareName(name, Product.class))
        {
            operator = parseProduct(element);
        }
        else if (compareName(name, Project.class))
        {
            operator = parseProject(element);
        }
        else if (compareName(name, Select.class))
        {
            operator = parseSelect(element);
        }
        else if (compareName(name, SemiJoin.class))
        {
            operator = parseSemiJoin(element);
        }
        else if (compareName(name, SendPlan.class))
        {
            operator = parseSendPlan(element);
        }
        else if (name.equalsIgnoreCase(SetOpType.UNION.name()) || name.equalsIgnoreCase(SetOpType.INTERSECT.name())
                || name.equalsIgnoreCase(SetOpType.EXCEPT.name()) || name.equalsIgnoreCase(SetOpType.OUTER_UNION.name()))
        {
            operator = parseSetOperator(element);
        }
        else if (compareName(name, Sort.class))
        {
            operator = parseSort(element);
        }
        else if (compareName(name, CreateDataObject.class))
        {
            operator = parseCreateDataObject(element);
        }
        else if (compareName(name, DropDataObject.class))
        {
            operator = parseDropSchemaObject(element);
        }
        else if (compareName(name, Insert.class))
        {
            operator = parseInsert(element);
        }
        else if (compareName(name, Delete.class))
        {
            operator = parseDelete(element);
        }
        else if (compareName(name, Update.class))
        {
            operator = parseUpdate(element);
        }
        else if (compareName(name, Placeholder.class))
        {
            operator = new Placeholder();
        }
        else
        {
            throw new UnsupportedOperationException();
        }
        
        // Set execution data source
        if (!(operator instanceof SendPlan))
        {
            String execution_data_source = element.getAttribute(PlanXmlSerializer.EXECUTION_ATTR);
            if (!execution_data_source.isEmpty())
            {
                operator.setExecutionDataSourceName(execution_data_source);
            }
        }
        
        // Set cardinality estimate
        String cardinality_estimate_str = element.getAttribute(PlanXmlSerializer.ESTIMATE_ATTR);
        if (!cardinality_estimate_str.isEmpty())
        {
            Size cardinality_estimate = null;
            try
            {
                cardinality_estimate = Size.constantOf(cardinality_estimate_str);
            }
            catch (DataSourceException e)
            {
                // Chain the exception
                String s = StringUtil.format("Error parsing cardinality estimate");
                throw new XmlParserException(s, e);
            }
            assert cardinality_estimate != null;
            operator.setCardinalityEstimate(cardinality_estimate);
        }
        
        return operator;
    }
    
    /**
     * Parses an element to create an update operator.
     * 
     * @param element
     *            the specified element
     * @return the created update operator.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Operator parseUpdate(Element element) throws XmlParserException
    {
        Operator child = getChildOperator(element);
        assert child != null;
        
        String target_data_source = getNonEmptyAttribute(element, PlanXmlSerializer.TARGET_DATA_SOURCE_ATTR);
        String update_var_str = getAttribute(element, PlanXmlSerializer.UPDATE_VAR_ATTR);
        
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        
        Element target_elm = getOnlyChildElement(args_elm, PlanXmlSerializer.TARGET_ELM);
        Term target = this.parseTerm(getOnlyChildElement(target_elm));
        
        Scan scan = null;
        RelativeVariable var = null;
        if (update_var_str != null) var = new RelativeVariable(update_var_str);
        Update op = new Update(target_data_source, target, var, scan);
        op.addChild(child);
        
        List<Element> assignment_elms = getChildElements(args_elm, PlanXmlSerializer.ASSIGNMENT_ELM);
        for (Element assignment_elm : assignment_elms)
        {
            target_elm = getOnlyChildElement(assignment_elm, PlanXmlSerializer.TARGET_ELM);
            target = this.parseTerm(getOnlyChildElement(target_elm));
            
            Element plan_elm = getOnlyChildElement(assignment_elm, PlanXmlSerializer.QUERY_PLAN_ELM);
            UnifiedApplicationState uas = QueryProcessorFactory.getInstance().getUnifiedApplicationState();
            LogicalPlan plan = parseLogicalPlan(uas, plan_elm);
            
            op.addAssignment(new Update.Assignment(target, plan));
        }
        
        return op;
    }
    
    /**
     * Parses an element to create a delete operator.
     * 
     * @param element
     *            the specified element
     * @return the created delete operator
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Operator parseDelete(Element element) throws XmlParserException
    {
        Operator child = getChildOperator(element);
        assert child != null;
        
        String target_data_source = getNonEmptyAttribute(element, PlanXmlSerializer.TARGET_DATA_SOURCE_ATTR);
        String delete_var_str = getNonEmptyAttribute(element, PlanXmlSerializer.DELETE_VAR_ATTR);
        
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        
        Element target_elm = getOnlyChildElement(args_elm, PlanXmlSerializer.TARGET_ELM);
        Term target = this.parseTerm(getOnlyChildElement(target_elm));
        
        Scan scan = null;
        Delete op = new Delete(target_data_source, target, new RelativeVariable(delete_var_str), scan);
        op.addChild(child);
        
        return op;
    }
    
    /**
     * Parses an element to create an insert operator.
     * 
     * @param element
     *            the specified element
     * @return the created insert operator
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Operator parseInsert(Element element) throws XmlParserException
    {
        Operator child = getChildOperator(element);
        assert child != null;
        
        String target_data_source = getNonEmptyAttribute(element, PlanXmlSerializer.TARGET_DATA_SOURCE_ATTR);
        
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        
        Element target_elm = getOnlyChildElement(args_elm, PlanXmlSerializer.TARGET_ELM);
        Term target = this.parseTerm(getOnlyChildElement(target_elm));
        
        Insert op = new Insert(target_data_source, target);
        op.addChild(child);
        
        // Parse the target attributes
        List<String> target_attrs = new ArrayList<String>();
        Element attrs_elm = getOnlyChildElement(args_elm, PlanXmlSerializer.ATTRS_ELM);
        for (Element child_element : getChildElements(attrs_elm, PlanXmlSerializer.ATTR_ELM))
        {
            target_attrs.add(getNonEmptyAttribute(child_element, PlanXmlSerializer.NAME_ATTR));
        }
        op.setTargetAttributes(target_attrs);
        
        Element plan_elm = getOnlyChildElement(args_elm, PlanXmlSerializer.QUERY_PLAN_ELM);
        UnifiedApplicationState uas = QueryProcessorFactory.getInstance().getUnifiedApplicationState();
        LogicalPlan plan = parseLogicalPlan(uas, plan_elm);
        
        op.setInsertPlan(plan);
        
        return op;
    }
    
    /**
     * Parses an element to create a drop schema object operator.
     * 
     * @param element
     *            the specified element
     * @return the created drop schema object operator
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Operator parseDropSchemaObject(Element element) throws XmlParserException
    {
        String schema_name = getNonEmptyAttribute(element, PlanXmlSerializer.SCHEMA_OBJECT_ATTR);
        String data_source_name = getNonEmptyAttribute(element, PlanXmlSerializer.DATA_SOURCE_ATTR);
        return new DropDataObject(schema_name, data_source_name);
    }
    
    /**
     * Parses an element to create a create data object operator.
     * 
     * @param element
     *            the specified element
     * @return the created data object operator
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Operator parseCreateDataObject(Element element) throws XmlParserException
    {
        String schema_name = getNonEmptyAttribute(element, PlanXmlSerializer.SCHEMA_OBJECT_ATTR);
        String data_source_name = getNonEmptyAttribute(element, PlanXmlSerializer.DATA_SOURCE_ATTR);
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        Element schema_tree_elm = getOptionalChildElement(args_elm, TypeXmlSerializer.SCHEMA_TREE_ELM);
        SchemaTree schema_tree = TypeXmlParser.parseSchemaTree(schema_tree_elm);
        CreateDataObject op = new CreateDataObject(schema_name, schema_tree, data_source_name);
        for (Element default_plan_elm : getChildElements(args_elm, PlanXmlSerializer.QUERY_PLAN_ELM))
        {
            String path_str = default_plan_elm.getAttribute(PlanXmlSerializer.TYPE_ATTR);
            SchemaPath path;
            try
            {
                path = new SchemaPath(path_str);
            }
            catch (ParseException e)
            {
                throw new XmlParserException("Error parsing schema path", e);
            }
            
            UnifiedApplicationState uas = QueryProcessorFactory.getInstance().getUnifiedApplicationState();
            LogicalPlan plan = parseLogicalPlan(uas, default_plan_elm);
            
            op.addDefaultPlan(path, plan);
        }
        
        return op;
    }
    
    /**
     * Parses an element to create an offset fetch operator.
     * 
     * @param element
     *            the specified element.
     * @return the created offset fetch operator
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Operator parseOffsetFetch(Element element) throws XmlParserException
    {
        Operator child_operator = getChildOperator(element);
        OffsetFetch offset_fetch = new OffsetFetch();
        offset_fetch.addChild(child_operator);
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        Element offset_elm = getOptionalChildElement(args_elm, PlanXmlSerializer.OFFSET_ELM);
        if (offset_elm != null)
        {
            Term offset = parseTerm(getOnlyChildElement(offset_elm));
            offset_fetch.setOffset(offset);
        }
        Element fetch_elm = getOptionalChildElement(args_elm, PlanXmlSerializer.FETCH_ELM);
        if (fetch_elm != null)
        {
            Term fetch = parseTerm(getOnlyChildElement(fetch_elm));
            offset_fetch.setFetch(fetch);
        }
        
        return offset_fetch;
    }
    
    /**
     * Parses an element to create a sort operator.
     * 
     * @param element
     *            the specified element.
     * @return the created sort operator.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Operator parseSort(Element element) throws XmlParserException
    {
        Operator child_operator = getChildOperator(element);
        
        Sort sort = new Sort();
        sort.addChild(child_operator);
        
        // Add sort items
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        for (Element child_element : getChildElements(args_elm))
        {
            if (compareName(child_element.getTagName(), Sort.Item.class))
            {
                Term term = parseTerm(getOnlyChildElement(child_element));
                Spec spec = Spec.valueOf(child_element.getAttribute(PlanXmlSerializer.SORT_SPEC_ATTR));
                Nulls nulls = Nulls.valueOf(child_element.getAttribute(PlanXmlSerializer.SORT_NULLS_ATTR));
                sort.addSortItem(term, spec, nulls);
            }
        }
        return sort;
    }
    
    /**
     * Parses a set operator from the specified element.
     * 
     * @param element
     *            the element
     * @return the created set operator.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Operator parseSetOperator(Element element) throws XmlParserException
    {
        List<Operator> child_operators = getChildOperators(element);
        SetOpType type = SetOpType.valueOf(element.getTagName().toUpperCase());
        String set_quantifier_str = getNonEmptyAttribute(element, PlanXmlSerializer.SET_QUANTIFIER_ATTR);
        SetQuantifier set_quantifier = (set_quantifier_str.equals(SetQuantifier.ALL.name()))
                ? SetQuantifier.ALL
                : SetQuantifier.DISTINCT;
        
        SetOperator op = new SetOperator(type, set_quantifier);
        for (Operator child : child_operators)
            op.addChild(child);
        
        return op;
    }
    
    /**
     * Creates the operator of group by.
     * 
     * @param element
     *            the element
     * @return the created operator
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Operator parseGroupBy(Element element) throws XmlParserException
    {
        Operator child = getChildOperator(element);
        GroupBy group_by = new GroupBy();
        group_by.addChild(child);
        
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        for (Element child_element : getChildElements(args_elm))
        {
            // Add group by terms
            if (compareName(child_element.getTagName(), GroupByItem.class))
            {
                String alias = child_element.getAttribute(PlanXmlSerializer.ATTR_NAME_ATTR);
                Term term = null;
                for (Element child_group_by : getChildElements(child_element))
                {
                    term = parseTerm(child_group_by);
                }
                group_by.addGroupByTerm(term, alias);
            }
            
            // Add carry on terms
            if (compareName(child_element.getTagName(), GroupBy.CARRY_ON_TAG))
            {
                RelativeVariable variable = null;
                for (Element child_group_by : getChildElements(child_element))
                {
                    assert (compareName(child_group_by.getTagName(), Variable.class));
                    Term term = parseTerm(child_group_by);
                    assert (term instanceof RelativeVariable);
                    variable = (RelativeVariable) term;
                }
                group_by.addCarryOnTerm(variable);
            }
            
            // Add aggregates
            if (compareName(child_element.getTagName(), Aggregate.class))
            {
                String call_alias = getAlias(child_element);
                AggregateFunctionCall call = parseAggregateFunctionCall(getOnlyChildElement(child_element));
                assert call != null;
                group_by.addAggregate(call, call_alias);
            }
        }
        return group_by;
    }
    
    /**
     * Creates the operator of partition by.
     * 
     * @param element
     *            the element
     * @return the created operator
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Operator parsePartitionBy(Element element) throws XmlParserException
    {
        Operator child = getChildOperator(element);
        PartitionBy partition_by = new PartitionBy();
        partition_by.addChild(child);
        
        // Add limit number
        String limit = getAttribute(element, PlanXmlSerializer.LIMIT_ATTR);
        if (limit != null) partition_by.setLimit(new IntegerValue(Integer.parseInt(limit)));
        
        String rank_alias = getAttribute(element, PlanXmlSerializer.RANK_ALIAS_ATTR);
        partition_by.setRankAlias(rank_alias);
        
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        for (Element child_element : getChildElements(args_elm))
        {
            
            // Add partition by terms
            if (compareName(child_element.getTagName(), PlanXmlSerializer.PARTITION_BY_ELM))
            {
                RelativeVariable variable = null;
                for (Element child_partition_by : getChildElements(child_element))
                {
                    assert (compareName(child_partition_by.getTagName(), Variable.class));
                    Term term = parseTerm(child_partition_by);
                    assert (term instanceof RelativeVariable);
                    variable = (RelativeVariable) term;
                    partition_by.addPartitionByTerm(variable);
                }
            }
            
            if (compareName(child_element.getTagName(), Sort.Item.class))
            {
                Sort dummy = new Sort();
                Term term = parseTerm(getOnlyChildElement(child_element));
                Spec spec = Spec.valueOf(child_element.getAttribute(PlanXmlSerializer.SORT_SPEC_ATTR));
                Nulls nulls = Nulls.valueOf(child_element.getAttribute(PlanXmlSerializer.SORT_NULLS_ATTR));
                dummy.addSortItem(term, spec, nulls);
                
                partition_by.addSortByItem(dummy.getSortItems().get(0));
            }
            
        }
        return partition_by;
    }
    
    /**
     * Creates the partition by operator implementation specified by the element.
     * 
     * @param element
     *            the element
     * @param operator
     *            the logical operator of group by
     * @return the created group by operator implementation
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private OperatorImpl parsePartitionByImpl(Element element, PartitionBy operator) throws XmlParserException
    {
        OperatorImpl child_opim = getChildOperatorImpl(element, operator.getChild());
        return new PartitionByImpl(operator, child_opim);
    }
    
    /**
     * Creates the operator of outer join.
     * 
     * @param element
     *            the element
     * @return the created operator
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Operator parseOuterJoin(Element element) throws XmlParserException
    {
        List<Operator> child_operators = getChildOperators(element);
        Variation variation = Variation.valueOf(getNonEmptyAttribute(element, PlanXmlSerializer.VARIATION_ATTR));
        OuterJoin join = new OuterJoin(variation);
        for (Operator child : child_operators)
            join.addChild(child);
        
        // Add join conditions
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        for (Element child_element : getChildElements(args_elm))
        {
            Term condition = parseTerm(child_element);
            if (condition != null) join.addCondition(condition);
        }
        return join;
    }
    
    /**
     * Compares if the given tag name equals the name of the given class. The comparison is case insensitive and removes the
     * underscores from the tag name.
     * 
     * @param tag_name
     *            the tag name to be compared.
     * @param clazz
     *            the class to compare.
     * @return <code>true</code> if the given name equals the name of the given class; <code>false</code> otherwise.
     */
    private boolean compareName(String tag_name, Class<? extends Object> clazz)
    {
        String expected = SystemUtil.getSimpleName(clazz);
        // Remove underscores from tag name
        String new_name = tag_name.replaceAll("_", "");
        
        return new_name.equalsIgnoreCase(expected);
    }
    
    /**
     * Compares if the given tag name equals the givwn string. The comparison is case insensitive and removes the underscores from
     * the tag name.
     * 
     * @param tag_name
     *            the tag name to be compared.
     * @param expected
     *            the strimg to compare.
     * @return <code>true</code> if the given name equals the name of the given class; <code>false</code> otherwise.
     */
    private boolean compareName(String tag_name, String expected)
    {
        // Remove underscores from tag name
        // String new_name = tag_name.replaceAll("_", "");
        
        return tag_name.equalsIgnoreCase(expected);
    }
    
    /**
     * Creates the operator for eliminating duplicate.
     * 
     * @param element
     *            the element
     * @return the created operator
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private EliminateDuplicates parseEliminateDuplicates(Element element) throws XmlParserException
    {
        Operator child = getChildOperator(element);
        assert child != null;
        EliminateDuplicates op = new EliminateDuplicates();
        op.addChild(child);
        
        return op;
    }
    
    /**
     * Creates the operator for send plan.
     * 
     * @param element
     *            the element
     * @return the created operator
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private SendPlan parseSendPlan(Element element) throws XmlParserException
    {
        String data_source_name = element.getAttribute(PlanXmlSerializer.EXECUTION_ATTR);
        
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        LogicalPlan sent_plan = parseLogicalPlan(args_elm);
        
        SendPlan op = new SendPlan(data_source_name, sent_plan);
        
        // Add copy operators
        for (Operator child : getChildOperators(element))
        {
            // FIXME The SendPlan operator is supposed to have many Copy operators and at most one non-copy operator
            // assert (child instanceof Copy);
            
            op.addChild(child);
        }
        
        return op;
    }
    
    /**
     * Creates the operator for copy.
     * 
     * @param element
     *            the element
     * @return the created operator
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Copy parseCopy(Element element) throws XmlParserException
    {
        String target_data_src_name = element.getAttribute(PlanXmlSerializer.TARGET_DATA_SOURCE_ATTR);
        String target_schema_obj_name = element.getAttribute(PlanXmlSerializer.TARGET_SCHEMA_OBJECT_ATTR);
        
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        LogicalPlan copy_plan = parseLogicalPlan(args_elm);
        
        Copy op = new Copy(target_data_src_name, target_schema_obj_name, copy_plan);
        String data_src_name = element.getAttribute(PlanXmlSerializer.EXECUTION_ATTR);
        op.setExecutionDataSourceName(data_src_name);
        
        return op;
    }
    
    /**
     * Creates the operator for apply plan.
     * 
     * @param element
     *            the element
     * @return the created operator
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private ApplyPlan parseApplyPlan(Element element) throws XmlParserException
    {
        String attr_name = getNonEmptyAttribute(element, PlanXmlSerializer.ATTR_NAME_ATTR);
        
        Operator child = getChildOperator(element);
        assert child != null;
        
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        
        Term exec_condition = null;
        Element cond_elm = getOptionalChildElement(args_elm, PlanXmlSerializer.APPLYPLAN_EXEC_CONDITION);
        if (cond_elm != null)
        {
            exec_condition = parseTerm(getOnlyChildElement(cond_elm));
        }
        
        LogicalPlan apply_plan = parseLogicalPlan(args_elm);
        
        ApplyPlan op = new ApplyPlan(attr_name, apply_plan);
        if (exec_condition != null)
        {
            op.setExecutionCondition(exec_condition);
        }
        op.addChild(child);
        
        return op;
    }
    
    /**
     * Creates the operator for assign.
     * 
     * @param element
     *            the element
     * @return the created operator
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Assign parseAssign(Element element) throws XmlParserException
    {
        String target = getNonEmptyAttribute(element, PlanXmlSerializer.TARGET_ATTR);
        
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        LogicalPlan assigned_plan = parseLogicalPlan(args_elm);
        
        Assign assign = new Assign(assigned_plan, target);
        
        try
        {
            assign.updateOutputInfo();
            // Update the nested plan's output info, since need to use its output type
            assign.getPlan().updateOutputInfoDeep();
            LogicalPlanUtil.updateAssignedTempSchemaObject(assign);
        }
        catch (QueryExecutionException e)
        {
            throw new XmlParserException("Error parsing assign op: " + e.toString(), e);
        }
        catch (QueryCompilationException e)
        {
            throw new XmlParserException("Error parsing assign op: " + e.toString(), e);
        }
        return assign;
    }
    
    /**
     * Creates the operator for exists.
     * 
     * @param element
     *            the element
     * @return the created operator
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Exists parseExists(Element element) throws XmlParserException
    {
        String attr_name = getNonEmptyAttribute(element, PlanXmlSerializer.ATTR_NAME_ATTR);
        
        Operator child = getChildOperator(element);
        assert child != null;
        
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        
        Term exec_condition = null;
        Element cond_elm = getOptionalChildElement(args_elm, PlanXmlSerializer.APPLYPLAN_EXEC_CONDITION);
        if (cond_elm != null)
        {
            exec_condition = parseTerm(getOnlyChildElement(cond_elm));
        }
        
        LogicalPlan exist_plan = parseLogicalPlan(args_elm);
        
        Exists op = new Exists(attr_name, exist_plan);
        if (exec_condition != null)
        {
            op.setExecutionCondition(exec_condition);
        }
        op.addChild(child);
        
        return op;
    }
    
    /**
     * Creates the product operator.
     * 
     * @param element
     *            the element
     * @return the created product operator.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Product parseProduct(Element element) throws XmlParserException
    {
        List<Operator> child_operators = getChildOperators(element);
        Product op = new Product();
        for (Operator child : child_operators)
            op.addChild(child);
        
        return op;
    }
    
    /**
     * Creates the select operator.
     * 
     * @param element
     *            the element
     * @return the created select operator
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Select parseSelect(Element element) throws XmlParserException
    {
        Operator child = getChildOperator(element);
        assert (child != null);
        Select select = new Select();
        select.addChild(child);
        
        // Add selection conditions
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        for (Element child_element : getChildElements(args_elm))
        {
            Term condition = parseTerm(child_element);
            if (condition != null) select.addCondition(condition);
        }
        return select;
    }
    
    /**
     * Creates join operator.
     * 
     * @param element
     *            the element
     * @return the created join operator.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private InnerJoin parseInnerJoin(Element element) throws XmlParserException
    {
        List<Operator> child_operators = getChildOperators(element);
        InnerJoin join = new InnerJoin();
        for (Operator child : child_operators)
            join.addChild(child);
        
        // Add join conditions
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        for (Element child_element : getChildElements(args_elm))
        {
            Term condition = parseTerm(child_element);
            if (condition != null) join.addCondition(condition);
        }
        return join;
    }
    
    /**
     * Gets the child operators specified by the children elements of the given element.
     * 
     * @param element
     *            the element
     * @return the child operators
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private List<Operator> getChildOperators(Element element) throws XmlParserException
    {
        List<Operator> child_operators = new ArrayList<Operator>();
        // Get child operators
        for (Element child_element : getChildElements(element))
        {
            if (!child_element.getTagName().equalsIgnoreCase(PlanXmlSerializer.ARGS_ELM))
            {
                Operator operator = parseOperator(child_element);
                child_operators.add(operator);
            }
        }
        return child_operators;
    }
    
    /**
     * Creates semi-join operator.
     * 
     * @param element
     *            the element
     * @return the created semi-join operator.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private SemiJoin parseSemiJoin(Element element) throws XmlParserException
    {
        List<Operator> child_operators = getChildOperators(element);
        SemiJoin join = new SemiJoin();
        for (Operator child : child_operators)
            join.addChild(child);
        
        // Add join conditions
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        for (Element child_element : getChildElements(args_elm))
        {
            Term condition = parseTerm(child_element);
            if (condition != null) join.addCondition(condition);
        }
        return join;
    }
    
    /**
     * Creates anti-semi-join operator.
     * 
     * @param element
     *            the element
     * @return the created anti-semi-join operator.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private AntiSemiJoin parseAntiSemiJoin(Element element) throws XmlParserException
    {
        List<Operator> child_operators = getChildOperators(element);
        AntiSemiJoin join = new AntiSemiJoin();
        for (Operator child : child_operators)
            join.addChild(child);
        
        // Add join conditions
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        for (Element child_element : getChildElements(args_elm))
        {
            Term condition = parseTerm(child_element);
            if (condition != null) join.addCondition(condition);
        }
        return join;
    }
    
    /**
     * Creates project operator.
     * 
     * @param element
     *            the element
     * @return the created project operator
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Project parseProject(Element element) throws XmlParserException
    {
        Operator child = getChildOperator(element);
        Project project = new Project();
        project.addChild(child);
        
        // Add project items
        String outputs_elements = getAttribute(element, PlanXmlSerializer.SELECT_ELEMENT);
        if (outputs_elements != null)
        {
            if (Boolean.valueOf(outputs_elements))
            {
                project.setProjectQualifier(ProjectQualifier.ELEMENT);
            }
        }
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        for (Element child_element : getChildElements(args_elm))
        {
            if (compareName(child_element.getTagName(), Project.Item.class))
            {
                String item_alias = getAlias(child_element);
                Term term = parseTerm(getOnlyChildElement(child_element));
                assert term != null;
                try
                {
                    project.addProjectionItem(term, item_alias, true);
                }
                catch (QueryCompilationException e)
                {
                    String s = format("Duplicate projection alias: %s", item_alias);
                    throw new XmlParserException(s);
                }
            }
        }
        return project;
    }
    
    /**
     * Gets the only child operator specified by the element's child.
     * 
     * @param element
     *            the element.
     * @return the only child operator
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Operator getChildOperator(Element element) throws XmlParserException
    {
        Operator child = null;
        for (Element child_element : getChildElements(element))
        {
            if (!child_element.getTagName().equalsIgnoreCase(PlanXmlSerializer.ARGS_ELM)
                    && !child_element.getTagName().equalsIgnoreCase(PlanXmlSerializer.ASSIGN_LIST_ELM))
            {
                // Make sure there is only one child operator
                assert child == null;
                Operator operator = parseOperator(child_element);
                child = operator;
            }
        }
        
        return child;
    }
    
    /**
     * Creates index scan operator..
     * 
     * @param element
     *            the element
     * @return the created index scan operator
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private IndexScan parseIndexScan(Element element) throws XmlParserException
    {
        String alias = getAlias(element);
        
        String index_name = getAttribute(element, PlanXmlSerializer.INDEX_NAME_ATTR);
        Operator child = getChildOperator(element);
        if (child == null)
        {
            // Add a data object ground child
            child = new Ground();
        }
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        Element path_elm = getOnlyChildElement(args_elm, PlanXmlSerializer.PATH_ELM);
        Term scan_term = parseTerm(getOnlyChildElement(path_elm));
        IndexScan object = new IndexScan(alias, scan_term, index_name, child);
        for (Element spec_elm : getChildElements(args_elm, PlanXmlSerializer.KEY_RANGE_ELM))
        {
            Element lower_elm = getOptionalChildElement(spec_elm, PlanXmlSerializer.LOWER_ELM);
            Element upper_elm = getOptionalChildElement(spec_elm, PlanXmlSerializer.UPPER_ELM);
            Term lower_term = null;
            Term upper_term = null;
            boolean lower_open = false;
            boolean upper_open = false;
            if (lower_elm != null)
            {
                lower_term = parseTerm(getOnlyChildElement(lower_elm));
                lower_open = Boolean.parseBoolean(getAttribute(lower_elm, PlanXmlSerializer.IS_OPEN_ATTR));
            }
            if (upper_elm != null)
            {
                upper_term = parseTerm(getOnlyChildElement(upper_elm));
                upper_open = Boolean.parseBoolean(getAttribute(upper_elm, PlanXmlSerializer.IS_OPEN_ATTR));
            }
            KeyRangeSpec spec = new IndexScan.KeyRangeSpec(lower_term, upper_term, lower_open, upper_open);
            object.addKeyRangeSpec(spec);
        }
        
        return object;
    }
    
    /**
     * Creates scan operator.
     * 
     * @param element
     *            the element
     * @return the created scan operator.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Scan parseScan(Element element) throws XmlParserException
    {
        String alias = getAlias(element);
        String position_var = getPositionVariable(element);
        
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        List<Element> arguments = getChildElements(args_elm);
        Term scan_term = parseTerm(arguments.remove(0));
        
        Operator child = getChildOperator(element);
        if (child == null)
        {
            // Add a data object ground child
            child = new Ground();
        }
        
        Scan object = new Scan(alias, scan_term, child);
        
        if (position_var != null)
        {
            object.setOrderVariable(new PositionVariable(position_var));
        }
        
        String flatten_semantics = getAttribute(element, PlanXmlSerializer.VARIATION_ATTR);
        if (flatten_semantics != null)
        {
            object.setFlattenSemantics(FlattenSemantics.valueOf(flatten_semantics));
        }
        
        return object;
    }
    
    /**
     * Creates Subquery operator.
     * 
     * @param element
     *            the element
     * @return the created Subquery operator.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Subquery parseSubquery(Element element) throws XmlParserException
    {
        String alias = getAlias(element);
        String position_var = getPositionVariable(element);
        
        Operator child = getChildOperator(element);
        
        Subquery object = new Subquery(alias);
        object.addChild(child);
        
        if (position_var != null)
        {
            object.setOrderVariable(new PositionVariable(position_var));
        }
        
        return object;
    }
    
    /**
     * Parses a SubqueryImpl operator.
     * 
     * @param element
     *            the element
     * @param operator
     *            the parsed Subquery operator
     * @return the parsed SubqueryImpl operator
     * @throws XmlParserException
     *             if an error occurs
     */
    private OperatorImpl parseSubqueryImpl(Element element, Subquery operator) throws XmlParserException
    {
        OperatorImpl child_opim = getChildOperatorImpl(element, operator.getChildren().get(0));
        return new SubqueryImpl(operator, child_opim);
    }
    
    /**
     * Creates navigate operator.
     * 
     * @param element
     *            the element
     * @return the created navigate operator.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Navigate parseNavigate(Element element) throws XmlParserException
    {
        String alias = getAlias(element);
        
        Element args_elm = getOnlyChildElement(element, PlanXmlSerializer.ARGS_ELM);
        List<Element> arguments = getChildElements(args_elm);
        Term navigate_term = parseTerm(arguments.remove(0));
        
        Operator child = getChildOperator(element);
        if (child == null)
        {
            // Add a data object ground child
            child = new Ground();
        }
        
        Navigate object = new Navigate(alias, navigate_term, child);
        
        return object;
    }
    
    /**
     * Creates data object ground.
     * 
     * @param element
     *            the element
     * @return the created data object ground
     */
    private Ground parseGround(Element element)
    {
        Ground object = new Ground();
        return object;
    }
    
    /**
     * Creates a term from a given element.
     * 
     * @param element
     *            the element
     * @return the created term; returns <code>null</code> if the element does not specify a term
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Term parseTerm(Element element) throws XmlParserException
    {
        Term result = null;
        String name = element.getTagName();
        
        if (compareName(name, Constant.class))
        {
            result = parseConstant(element);
        }
        else if (compareName(name, FunctionCall.class))
        {
            result = parseFunctionCall(element);
        }
        else if (compareName(name, QueryPath.class))
        {
            result = parseQueryPath(element);
        }
        else if (compareName(name, Parameter.class))
        {
            result = parseParameter(element);
        }
        else if (compareName(name, ElementVariable.class))
        {
            result = parseVariable(element);
        }
        else if (compareName(name, PositionVariable.class))
        {
            result = parseVariable(element);
        }
        else if (compareName(name, Variable.class))
        {
            result = parseVariable(element);
        }
        else
        {
            throw new UnsupportedOperationException();
        }
        
        String term_type = element.getAttribute(PlanXmlSerializer.TYPE_ATTR);
        
        if (term_type != null && !term_type.isEmpty())
        {
            try
            {
                ((AbstractTerm) result).setType(TypeEnum.get(term_type));
            }
            catch (TypeException e)
            {
                String s = format("Unknown parameter type name: %s", term_type);
                throw new XmlParserException(s);
            }
        }
        else
        {
            Element type_elm = getOptionalChildElement(element, PlanXmlSerializer.TYPE_ATTR);
            if (type_elm != null)
            {
                ((AbstractTerm) result).setType(TypeXmlParser.parseType(type_elm));
            }
        }
        
        return result;
    }
    
    /**
     * Creates a constant that contains a scalar value.
     * 
     * @param element
     *            the element
     * @return the created constant
     * @throws XmlParserException
     *             when the constant type is unknown.
     */
    private Constant parseConstant(Element element) throws XmlParserException
    {
        Element child_element = getOnlyChildElement(element);
        String type_str = child_element.getTagName();
        Type type = null;
        try
        {
            type = TypeEnum.get(type_str);
        }
        catch (TypeException e1)
        {
            String s = format("Unknown constant type name: %s", type_str);
            throw new XmlParserException(s);
        }
        
        assert (type instanceof ScalarType || type instanceof NullType);
        
        try
        {
            String null_attr = child_element.getAttribute(PlanXmlSerializer.NULL_VALUE);
            
            Value out_value = null;
            
            // A null value is represented with a null="true" attribute
            if (!DomApiProxy.isEmpty(null_attr))
            {
                out_value = new NullValue();
            }
            else
            {
                String in_value = DomApiProxy.getNodeValue(child_element);
                
                out_value = (ScalarValue) ValueXmlParser.parseScalarValue((ScalarType) type, in_value);
            }
            
            Constant constant = new Constant(out_value);
            constant.setType(type);
            
            return constant;
        }
        catch (XmlParserException e)
        {
            throw new AssertionError(e);
        }
    }
    
    /**
     * Creates a function call.
     * 
     * @param element
     *            the element.
     * @return the created function call.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Term parseFunctionCall(Element element) throws XmlParserException
    {
        String function_name = getNonEmptyAttribute(element, PlanXmlSerializer.NAME_ATTR);
        String target_data_source_name = null;
        if (element.getAttribute(PlanXmlSerializer.TARGET_DATA_SOURCE_ATTR) != null
                && !element.getAttribute(PlanXmlSerializer.TARGET_DATA_SOURCE_ATTR).isEmpty())
        {
            target_data_source_name = getNonEmptyAttribute(element, PlanXmlSerializer.TARGET_DATA_SOURCE_ATTR);
        }
        
        Function function = null;
        
        try
        {
            if (target_data_source_name != null)
            {
                UnifiedApplicationState uas = QueryProcessorFactory.getInstance().getUnifiedApplicationState();
                function = uas.getDataSource(target_data_source_name).getExternalFunction(function_name);
            }
            else
            {
                function = FunctionRegistry.getInstance().getFunction(function_name);
            }
        }
        catch (FunctionRegistryException e)
        {
            // Chain the exception
            String s = format("Error parsing function call");
            throw new XmlParserException(s, e);
        }
        catch (DataSourceException e)
        {
            // Chain the exception
            String s = format("Error parsing function call");
            throw new XmlParserException(s, e);
        }
        
        // Parse the arguments
        List<Term> arguments = new ArrayList<Term>();
        for (Element child : getChildElements(element))
        {
            // Skip the type elements
            if (child.getTagName().equals(PlanXmlSerializer.TYPE_ATTR)) continue;
            
            Term argument = parseTerm(child);
            if (argument != null) arguments.add(argument);
        }
        
        if (function instanceof CastFunction)
        {
            String target_type_name = getNonEmptyAttribute(element, PlanXmlSerializer.TARGET_TYPE_ATTR);
            TypeEnum target_type = null;
            try
            {
                target_type = TypeEnum.getEntry(target_type_name);
            }
            catch (TypeException e)
            {
                String s = format("Unknown cast target type name: %s", target_type_name);
                throw new XmlParserException(s);
            }
            
            return new CastFunctionCall(arguments.get(0), target_type);
        }
        else if (function instanceof TupleFunction)
        {
            return new TupleFunctionCall(arguments);
        }
        else if (function instanceof CollectionFunction)
        {
            return new CollectionFunctionCall(arguments);
        }
        else if (function instanceof CaseFunction)
        {
            return new CaseFunctionCall(arguments);
        }
        else if (function instanceof ExternalFunction)
        {
            return new ExternalFunctionCall((ExternalFunction) function, arguments);
        }
        else if (function instanceof GeneralFunction)
        {
            return new GeneralFunctionCall((GeneralFunction) function, arguments);
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }
    
    /**
     * Creates an aggregate function call.
     * 
     * @param element
     *            the element.
     * @return the created aggregate function call.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private AggregateFunctionCall parseAggregateFunctionCall(Element element) throws XmlParserException
    {
        String function_name = getNonEmptyAttribute(element, PlanXmlSerializer.NAME_ATTR);
        Function function = null;
        try
        {
            function = FunctionRegistry.getInstance().getFunction(function_name);
        }
        catch (FunctionRegistryException e)
        {
            // Chain the exception
            String s = format("Error parsing aggregate function call");
            throw new XmlParserException(s, e);
        }
        
        if (function instanceof AggregateFunction)
        {
            String set_quantifier_str = getNonEmptyAttribute(element, PlanXmlSerializer.SET_QUANTIFIER_ATTR);
            SetQuantifier set_quantifier = (set_quantifier_str.equals(SetQuantifier.ALL.name()))
                    ? SetQuantifier.ALL
                    : SetQuantifier.DISTINCT;
            
            List<Term> arguments = new ArrayList<Term>();
            for (Element argument_element : getChildElements(element))
            {
                Term argument = parseTerm(argument_element);
                assert argument != null;
                arguments.add(argument);
            }
            
            return new AggregateFunctionCall((AggregateFunction) function, set_quantifier, arguments);
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }
    
    /**
     * Creates query path.
     * 
     * @param element
     *            the element
     * @return the created query path
     * @throws XmlParserException
     *             exception
     */
    private QueryPath parseQueryPath(Element element) throws XmlParserException
    {
        String steps = getNonEmptyAttribute(element, PlanXmlSerializer.STEPS_ATTR);
        Term term = parseTerm(getOnlyChildElement(element));
        QueryPath query_path = new QueryPath(term, Arrays.asList(steps.split(QueryPath.PATH_SEPARATOR_ESCAPED)));
        return query_path;
    }
    
    /**
     * Creates parameter.
     * 
     * @param element
     *            the element
     * @return the created parameter
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    private Parameter parseParameter(Element element) throws XmlParserException
    {
        Element term_elm = getOnlyChildElement(element);
        Term term = parseTerm(term_elm);
        
        try
        {
            term.inferType(Collections.<Operator> emptyList());
        }
        catch (QueryCompilationException e)
        {
            throw new RuntimeException(e);
        }
        
        String data_source = element.getAttribute(PlanXmlSerializer.DATA_SOURCE_ATTR);
        String schema_object = element.getAttribute(PlanXmlSerializer.SCHEMA_OBJECT_ATTR);
        
        Parameter param = (data_source.isEmpty() || schema_object.isEmpty()) ? new Parameter(term) : new Parameter(term,
                                                                                                                   data_source,
                                                                                                                   schema_object);
        
        return param;
    }
    
    /**
     * Creates variable.
     * 
     * @param element
     *            the element
     * @return the created variable
     */
    private Variable parseVariable(Element element)
    {
        VariableType mode = null;
        if (element.getAttribute(PlanXmlSerializer.MODE_ATTR) == null
                || element.getAttribute(PlanXmlSerializer.MODE_ATTR).isEmpty())
        {
            mode = VariableType.RELATIVE;
        }
        else
        {
            mode = VariableType.valueOf(element.getAttribute(PlanXmlSerializer.MODE_ATTR));
        }
        
        switch (mode)
        {
            case ABSOLUTE:
                String data_source = getNonEmptyAttribute(element, PlanXmlSerializer.DATA_SOURCE_ATTR);
                String schema_object = getNonEmptyAttribute(element, PlanXmlSerializer.SCHEMA_OBJECT_ATTR);
                return new AbsoluteVariable(data_source, schema_object);
            case RELATIVE:
                String rel_name = getNonEmptyAttribute(element, PlanXmlSerializer.NAME_ATTR);
                RelativeVariable rel_variable = new RelativeVariable(rel_name);
                return rel_variable;
            case ELEMENT:
                String elem_name = getNonEmptyAttribute(element, PlanXmlSerializer.NAME_ATTR);
                ElementVariable elem_variable = new ElementVariable(elem_name);
                return elem_variable;
            case POSITION:
                String pos_name = getNonEmptyAttribute(element, PlanXmlSerializer.NAME_ATTR);
                PositionVariable pos_variable = new PositionVariable(pos_name);
                return pos_variable;
            default:
                throw new AssertionError();
        }
    }
    
    /**
     * Gets the alias attribute from the element.
     * 
     * @param element
     *            the element
     * @return the attribute value for 'alias'; returns <code>null</code> if the alias is empty.
     */
    private String getAlias(Element element)
    {
        String alias = element.getAttribute(PlanXmlSerializer.ALIAS_ATTR);
        if (alias.isEmpty()) return null;
        return alias;
    }
    
    /**
     * Gets the position attribute from the element.
     * 
     * @param element
     *            the element
     * @return the attribute value for 'position_var'; returns <code>null</code> if the alias is empty.
     */
    private String getPositionVariable(Element element)
    {
        String alias = element.getAttribute(PlanXmlSerializer.POSITION_ATTR);
        if (alias.isEmpty()) return null;
        return alias;
    }
    
    /**
     * Builds the physical plan from the XML input.
     * 
     * @param uas
     *            the unified application state that may be used by the plan.
     * @param query_plan_elm
     *            the query plan element of the XML DOM.
     * @return the built physical query plan.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    public PhysicalPlan parsePhysicalPlan(UnifiedApplicationState uas, Element query_plan_elm) throws XmlParserException
    {
        m_target_assign_map.clear();
        
        assert (query_plan_elm.getTagName().equals(PlanXmlSerializer.QUERY_PLAN_ELM));
        
        QueryProcessorFactory.getInstance().setUnifiedApplicationState(uas);
        
        // Build the logical plan first
        LogicalPlan logical_plan = parseLogicalPlan(uas, query_plan_elm);
        
        List<Element> elms = getChildElements(query_plan_elm);
        assert elms.size() == 1 || elms.size() == 2;
        Element root_elm = null;
        if (elms.size() == 1) root_elm = elms.get(0);
        if (elms.size() == 2) root_elm = elms.get(1);
        
        if (root_elm.getAttribute(PlanXmlSerializer.IMPL_ATTR).isEmpty())
        {
            return null;
        }
        
        // Build the physical plan only if the execution data source attributes are set
        return parsePhysicalPlan(query_plan_elm, logical_plan);
    }
    
    private PhysicalPlan parsePhysicalPlan(Element query_plan_elm, LogicalPlan logical_plan) throws XmlParserException
    {
        List<Element> elms = getChildElements(query_plan_elm);
        assert elms.size() == 1 || elms.size() == 2;
        Element root_elm = null;
        if (elms.size() == 1) root_elm = elms.get(0);
        if (elms.size() == 2) root_elm = elms.get(1);
        
        // Parse the Assign operators
        Element assign_list_elm = getOptionalChildElement(query_plan_elm, PlanXmlSerializer.ASSIGN_LIST_ELM);
        List<AbstractAssignImpl> assign_impls = new ArrayList<AbstractAssignImpl>();
        if (assign_list_elm != null)
        {
            List<Assign> assigns = logical_plan.getAssigns();
            List<Element> assign_elms = getChildElements(assign_list_elm);
            assert assigns.size() == assign_elms.size();
            
            Iterator<Assign> assign_itr = assigns.iterator();
            Iterator<Element> assign_elm_itr = assign_elms.iterator();
            
            while (assign_elm_itr.hasNext())
            {
                Element assign_impl_elm = assign_elm_itr.next();
                Assign assign = assign_itr.next();
                AbstractAssignImpl assign_impl = (AbstractAssignImpl) parseOperatorImpl(assign_impl_elm, assign);
                assign_impls.add(assign_impl);
            }
        }
        
        OperatorImpl root_impl = parseOperatorImpl(root_elm, logical_plan.getRootOperator());
        PhysicalPlan plan = new PhysicalPlan(root_impl, logical_plan);
        
        for (AbstractAssignImpl assign_impl : assign_impls)
        {
            plan.addAssignImpl(assign_impl);
        }
        
        return plan;
    }
    
    /**
     * Builds the logical plan from the XML input.
     * 
     * @param uas
     *            the unified application state that may be used by the plan.
     * @param query_plan_elm
     *            the query plan element of the XML DOM.
     * @return the built logical query plan.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    public LogicalPlan parseLogicalPlan(UnifiedApplicationState uas, Element query_plan_elm) throws XmlParserException
    {
        QueryProcessorFactory.getInstance().setUnifiedApplicationState(uas);
        
        assert (query_plan_elm.getTagName().equals(PlanXmlSerializer.QUERY_PLAN_ELM));
        LogicalPlan plan = parseLogicalPlan(query_plan_elm);
        
        // Set the wrapping flag
        String wrapping_str = query_plan_elm.getAttribute(PlanXmlSerializer.WRAPPING_ATTR);
        plan.setWrapping((wrapping_str.isEmpty()) ? false : Boolean.valueOf(wrapping_str));
        
        try
        {
            plan.updateOutputInfoDeep();
        }
        catch (QueryCompilationException e)
        {
            // Chain the exception
            throw new XmlParserException("Error parsing logical plan: " + e.toString(), e);
        }
        
        return plan;
    }
    
    private LogicalPlan parseLogicalPlan(Element plan_elm) throws XmlParserException
    {
        List<Element> elms = getChildElements(plan_elm);
        assert elms.size() == 1 || elms.size() == 2;
        Element root_elm = null;
        if (elms.size() == 1) root_elm = elms.get(0);
        if (elms.size() == 2) root_elm = elms.get(1);
        
        // Parse the Assign operators
        Element assign_list_elm = getOptionalChildElement(plan_elm, PlanXmlSerializer.ASSIGN_LIST_ELM);
        
        List<Assign> assigns = new ArrayList<Assign>();
        if (assign_list_elm != null)
        {
            for (Element assign_elm : getChildElements(assign_list_elm))
            {
                Assign assign = (Assign) parseOperator(assign_elm);
                assigns.add(assign);
            }
        }
        
        // Parse the root operator
        Operator root = parseOperator(root_elm);
        assert root != null;
        LogicalPlan plan = new LogicalPlan(root);
        
        for (Assign assign : assigns)
        {
            plan.addAssignOperator(assign);
        }
        
        return plan;
    }
}
