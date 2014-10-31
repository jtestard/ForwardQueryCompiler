/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.Collections;
import org.testng.annotations.Test;

import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.xml.TypeXmlSerializer;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.visitors.ConsistencyChecker;
import edu.ucsd.forward.query.logical.visitors.DistributedNormalFormChecker;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.xml.PlanXmlSerializer;

/**
 * Tests physical DDL operator implementation.
 * 
 * @author Yupeng
 * 
 */
@Test
public class TestDdlOperatorImpl extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestDdlOperatorImpl.class);
    
    /**
     * Tests the implementation of schema objection definition.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testCreateDataObject() throws Exception
    {
        // Parse the test case from the XML file
        String location_str = "TestDdlOperatorImpl-testCreateDataObject.xml";
        parseTestCase(this.getClass(), location_str);
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        
        LogicalPlan logical_plan = getLogicalPlan(0);
        ConsistencyChecker.getInstance(uas).check(logical_plan);
        DistributedNormalFormChecker.getInstance(uas).check(logical_plan);
        
        // Check that the actual logical plan can be converted to physical plan
        PhysicalPlan actual = qp.generate(Collections.singletonList(logical_plan), uas).get(0);
        PhysicalPlan expected = getPhysicalPlan(0);
        
        // Check the logical to physical plan translation
        checkPhysicalPlan(actual, expected);
        
        // Check if the physical plan is evaluated correctly
        checkPlanEvaluation(expected, uas);
        
        // Check that the XML plan serializer and parser work for this operator implementation
        PlanXmlSerializer plan_serializer = new PlanXmlSerializer();
        Element element = plan_serializer.serializePhysicalPlan(expected);
        parsePhysicalPlan(element, uas, new LocationImpl(location_str));
        
        assertTrue(uas.getDataSource("src1").getSchemaObject("customers") != null);
    }
    
    public void testCreateDataObjectWithDefaultValue() throws Exception
    {
        // Parse the test case from the XML file
        String location_str = "TestDdlOperatorImpl-testCreateDataObjectWithDefaultValue.xml";
        parseTestCase(this.getClass(), location_str);
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        
        LogicalPlan logical_plan = getLogicalPlan(0);
        ConsistencyChecker.getInstance(uas).check(logical_plan);
        DistributedNormalFormChecker.getInstance(uas).check(logical_plan);
        
        // Check that the actual logical plan can be converted to physical plan
        PhysicalPlan actual = qp.generate(Collections.singletonList(logical_plan), uas).get(0);
        PhysicalPlan expected = getPhysicalPlan(0);
        
        // Check the logical to physical plan translation
        checkPhysicalPlan(actual, expected);
        
        // Check if the physical plan is evaluated correctly
        checkPlanEvaluation(expected, uas);
        
        // Check that the XML plan serializer and parser work for this operator implementation
        PlanXmlSerializer plan_serializer = new PlanXmlSerializer();
        Element element = plan_serializer.serializePhysicalPlan(expected);
        parsePhysicalPlan(element, uas, new LocationImpl(location_str));
        
        SchemaTree actual_tree = uas.getDataSource("src1").getSchemaObject("customers").getSchemaTree();
        String actual_str = TypeXmlSerializer.serializeSchemaTree(actual_tree);
        
        SchemaTree expected_tree = uas.getDataSource("expected").getSchemaObject("expected").getSchemaTree();
        String expected_str = TypeXmlSerializer.serializeSchemaTree(expected_tree);
        
        assertEquals(expected_str, actual_str);
    }
    
    
    /**
     * Tests the implementation of drop schema object.
     * 
     * @throws Exception
     *             when encounters error
     */
    public void testDropDataObject() throws Exception
    {
        // Parse the test case from the XML file
        String location_str = "TestDdlOperatorImpl-testDropDataObject.xml";
        parseTestCase(this.getClass(), location_str);
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        
        // The schema exists before the plan execution
        assertTrue(uas.getDataSource("src_1").hasSchemaObject("proposals"));
        
        LogicalPlan logical_plan = getLogicalPlan(0);
        ConsistencyChecker.getInstance(uas).check(logical_plan);
        DistributedNormalFormChecker.getInstance(uas).check(logical_plan);
        
        // Check that the actual logical plan can be converted to physical plan
        PhysicalPlan actual = qp.generate(Collections.singletonList(logical_plan), uas).get(0);
        PhysicalPlan expected = getPhysicalPlan(0);
        
        // Check the logical to physical plan translation
        checkPhysicalPlan(actual, expected);
        
        // Check if the physical plan is evaluated correctly
        checkPlanEvaluation(expected, uas);
        
        // Check that the XML plan serializer and parser work for this operator implementation
        PlanXmlSerializer plan_serializer = new PlanXmlSerializer();
        Element element = plan_serializer.serializePhysicalPlan(expected);
        parsePhysicalPlan(element, uas, new LocationImpl(location_str));
        
        assertFalse(uas.getDataSource("src_1").hasSchemaObject("proposals"));
    }
}
