/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;

import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.xml.PlanXmlSerializer;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * Tests physical operator implementation.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * @author Romain Vernoux
 */
@Test
public class TestOperatorImpl extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger  log   = Logger.getLogger(TestOperatorImpl.class);
    
    /**
     * If this flag is enabled, the testcases log information useful for creating/debugging test cases.
     */
    private static final boolean DEBUG = false;
    
    /**
     * Tests the term evaluators.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testTermEvaluation() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testTermEvaluation");
        checkOperatorImpl("TestOperatorImpl-testTermEvaluationCollectionFunction");
    }
    
    /**
     * Tests the implementation of scan.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testScanImpl() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testScanImpl");
        checkOperatorImpl("TestOperatorImpl-testScanImplEmpty");
    }
    
    /**
     * Tests the implementation of duplicates elimination.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testEliminateDuplicatesImpl() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testEliminateDuplicatesImpl");
    }
    
    /**
     * Tests the implementation of inner join using nested loops.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testInnerJoinImplNestedLoop() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testInnerJoinImplNestedLoop");
        checkOperatorImpl("TestOperatorImpl-testInnerJoinImplNestedLoopEmptyLeft");
        checkOperatorImpl("TestOperatorImpl-testInnerJoinImplNestedLoopEmptyRight");
        checkOperatorImpl("TestOperatorImpl-testInnerJoinImplNestedLoopEmptyBoth");
    }
    
    /**
     * Tests the implementation of full outer join implementation using nested loops.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testFullOuterJoinImplNestedLoop() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testFullOuterJoinImplNestedLoopHash");
        checkOperatorImpl("TestOperatorImpl-testFullOuterJoinImplNestedLoopHashEmptyLeft");
        checkOperatorImpl("TestOperatorImpl-testFullOuterJoinImplNestedLoopHashEmptyRight");
        checkOperatorImpl("TestOperatorImpl-testFullOuterJoinImplNestedLoopHashEmptyBoth");
    }
    
    /**
     * Tests the implementation of left outer join implementation using nested loops.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testLeftOuterJoinImplNestedLoop() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testLeftOuterJoinImplNestedLoopHash");
        checkOperatorImpl("TestOperatorImpl-testLeftOuterJoinImplNestedLoopHashEmptyLeft");
        checkOperatorImpl("TestOperatorImpl-testLeftOuterJoinImplNestedLoopHashEmptyRight");
        checkOperatorImpl("TestOperatorImpl-testLeftOuterJoinImplNestedLoopHashEmptyBoth");
        checkOperatorImpl("TestOperatorImpl-testLeftOuterJoinImplNestedLoopHashIsNullOn");
    }
    
    /**
     * Tests the implementation of right outer join implementation using nested loops.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testRightOuterJoinImplNestedLoop() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testRightOuterJoinImplNestedLoopHash");
        checkOperatorImpl("TestOperatorImpl-testRightOuterJoinImplNestedLoopHashEmptyLeft");
        checkOperatorImpl("TestOperatorImpl-testRightOuterJoinImplNestedLoopHashEmptyRight");
        checkOperatorImpl("TestOperatorImpl-testRightOuterJoinImplNestedLoopHashEmptyBoth");
    }
    
    /**
     * Tests the implementation of semi-join implementation using nested loops.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testSemiJoinImplNestedLoop() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testSemiJoinImplNestedLoopHash");
        checkOperatorImpl("TestOperatorImpl-testSemiJoinImplNestedLoopHashEmptyLeft");
        checkOperatorImpl("TestOperatorImpl-testSemiJoinImplNestedLoopHashEmptyRight");
        checkOperatorImpl("TestOperatorImpl-testSemiJoinImplNestedLoopHashEmptyBoth");
    }
    
    /**
     * Tests the implementation of anti-semi-join implementation using nested loops.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testAntiSemiJoinImplNestedLoop() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testAntiSemiJoinImplNestedLoopHash");
        checkOperatorImpl("TestOperatorImpl-testAntiSemiJoinImplNestedLoopHashEmptyLeft");
        checkOperatorImpl("TestOperatorImpl-testAntiSemiJoinImplNestedLoopHashEmptyRight");
        checkOperatorImpl("TestOperatorImpl-testAntiSemiJoinImplNestedLoopHashEmptyBoth");
    }
    
    /**
     * Tests the implementation of product by nested loop.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testProductImplNestedLoop() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testProductImplNestedLoop");
    }
    
    /**
     * Tests the implementation of project.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testProjectImpl() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testProjectImpl");
        checkOperatorImpl("TestOperatorImpl-testProjectImplEmpty");
        checkOperatorImpl("TestOperatorImpl-testProjectImplGround");
    }
    
    /**
     * Tests the implementation of select by sequential scanning.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testSelectImplSeq() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testSelectImplSeq");
        checkOperatorImpl("TestOperatorImpl-testSelectImplSeqEmpty");
    }
    
    /**
     * Tests the implementation of group by and aggregate functions.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testGroupByImpl() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testGroupByImplAll");
        checkOperatorImpl("TestOperatorImpl-testGroupByImplDistinct");
        checkOperatorImpl("TestOperatorImpl-testGroupByImplEmptyInput");
        checkOperatorImpl("TestOperatorImpl-testGroupByImplEmptyGroup");
        checkOperatorImpl("TestOperatorImpl-testGroupByImplMultipleTerms");
        checkOperatorImpl("TestOperatorImpl-testGroupByImplNoTerm");
        checkOperatorImpl("TestOperatorImpl-testGroupByImplNoTermEmptyInput");
    }
    
    /**
     * Tests the implementation of except.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testExceptImpl() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testExceptImpl");
        checkOperatorImpl("TestOperatorImpl-testExceptImplAll");
    }
    
    /**
     * Tests the implementation of intersect.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testIntersectImpl() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testIntersectImpl");
        checkOperatorImpl("TestOperatorImpl-testIntersectImplAll");
    }
    
    /**
     * Tests the implementation of union.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testUnionImpl() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testUnionImpl");
        checkOperatorImpl("TestOperatorImpl-testUnionImplAll");
    }
    
    /**
     * Tests the implementation of outer union.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testOuterUnionImpl() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testOuterUnionImpl");
        checkOperatorImpl("TestOperatorImpl-testOuterUnionImplAll");
        checkOperatorImpl("TestOperatorImpl-testOuterUnionImplWithSameSchema");
        checkOperatorImpl("TestOperatorImpl-testOuterUnionImplAllWithSameSchema");
    }
    
    /**
     * Tests the implementation of sort.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testSortImpl() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testSortImpl");
    }
    
    /**
     * Tests the implementation of offset fetch.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testOffsetFetchImpl() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testOffsetFetchImpl");
    }
    
    /**
     * Tests the implementation of apply plan.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testApplyPlanImpl() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testApplyPlanImpl");
        checkOperatorImpl("TestOperatorImpl-testApplyPlanImplEmpty");
        checkOperatorImpl("TestOperatorImpl-testApplyPlanImplNested");
    }
    
    /**
     * Tests the implementation of exists.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testExistsImpl() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testExistsImpl");
    }
    
    /**
     * Tests the implementation of exists of a simple structure.
     * 
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testExistsImplSimple() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testExistsImplSimple");
    }
    
    /**
     * Tests the implementation of assign implementation.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testAssignImpl() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testAssignImpl");
        checkOperatorImpl("TestOperatorImpl-testAssignImplNested");
        checkOperatorImpl("TestOperatorImpl-testNestedAssignImpl");
    }
    
    /**
     * Tests the implementation of partition-by implementation.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testPartitionByImpl() throws Exception
    {
        checkOperatorImpl("TestOperatorImpl-testPartitionByImpl");
        checkOperatorImpl("TestOperatorImpl-testPartitionByImplWithSort");
        checkOperatorImpl("TestOperatorImpl-testPartitionByImplWithRank");
        checkOperatorImpl("TestOperatorImpl-testPartitionByImplWithRankAndLimit"); // Romain: This is not SQL compliant, but works
                                                                                   // for now. I believe that the limit inside
                                                                                   // PartitionBy was required for an old version of
                                                                                   // the data access paper.
        checkOperatorImpl("TestOperatorImpl-testPartitionByImplEmptyInput");
    }
    
    /**
     * Tests the implementation of the index scan operator.
     * 
     * @throws Exception
     *             when encounters error
     */
    @Test(groups = AbstractTestCase.FAILURE)
    public void testIndexScan() throws Exception
    {
        // FIXME IndexScan operator needs to change according to new Scan
        checkOperatorImpl("TestOperatorImpl-testIndexScanOnly");
        checkOperatorImpl("TestOperatorImpl-testIndexScanUpperBoundOpen");
        checkOperatorImpl("TestOperatorImpl-testIndexScanUpperBoundClose");
        checkOperatorImpl("TestOperatorImpl-testIndexScanLowerBoundOpen");
        checkOperatorImpl("TestOperatorImpl-testIndexScanLowerBoundClose");
        checkOperatorImpl("TestOperatorImpl-testIndexScanFullRange");
    }
    
    /**
     * Tests the output of the physical plan meets the expectation.
     * 
     * @param relative_file_name
     *            the relative path of the XML file specifying the test case.
     * @throws Exception
     *             if an error occurs.
     */
    private void checkOperatorImpl(String relative_file_name) throws Exception
    {
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), relative_file_name + ".xml");
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        
        if (DEBUG)
        {
            
            // Parse the query
            String query_expr = getQueryExpression(0);
            List<AstTree> ast_trees = qp.parseQuery(query_expr, new LocationImpl(relative_file_name + ".xml"));
            assert (ast_trees.size() == 1);
            
            // Translate the AST
            LogicalPlan actual_input = qp.translate(Collections.singletonList(ast_trees.get(0)), uas).get(0);
            actual_input = qp.distribute(Collections.singletonList(actual_input), uas).get(0);
            
            // Check that the actual logical plan can be converted to physical plan
            PhysicalPlan physical = qp.generate(Collections.singletonList(actual_input), uas).get(0);
            
            logLogicalPlanExplain(actual_input);
            logPhysicalPlanXml(physical);
            
            checkLogicalPlan(actual_input, getLogicalPlan(0), uas);
        }
        
        LogicalPlan logical_plan = getLogicalPlan(0);
        
        // Check that the actual logical plan can be converted to physical plan
        PhysicalPlan actual = qp.generate(Collections.singletonList(logical_plan), uas).get(0);
        
        PhysicalPlan expected = getPhysicalPlan(0);
        
        Value output_value = qp.createEagerQueryResult(expected, uas).getValue();
        
        if (DEBUG)
        {
            logOutputValueXml(output_value);
            logOutputType(expected.getLogicalPlan().getOutputType());
        }
        
        // Check the logical to physical plan translation
        checkPhysicalPlan(actual, expected);
        
        // Check if the physical plan is evaluated correctly
        checkOutputValue(output_value, uas);
        
        // Re-evaluate the plan to check if the open() and close() methods work properly
        checkPlanEvaluation(expected, uas);
        
        // Check that the XML plan serializer and parser work for this operator implementation
        PlanXmlSerializer plan_serializer = new PlanXmlSerializer();
        Element element = plan_serializer.serializePhysicalPlan(expected);
        expected = parsePhysicalPlan(element, uas, new LocationImpl(relative_file_name));
        checkPlanEvaluation(expected, uas);
        
        // // Check if the physical plan can be copied
        // PhysicalPlan physical_copy = expected.copy();
        //
        // // Check if the copied physical plan is evaluated correctly
        // checkPlanEvaluation(physical_copy, uas);
    }
    
}
