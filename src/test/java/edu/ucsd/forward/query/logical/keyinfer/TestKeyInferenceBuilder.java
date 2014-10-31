/**
 * 
 */
package edu.ucsd.forward.query.logical.keyinfer;

import org.testng.annotations.Test;

import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceXmlParser;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.xml.TypeXmlSerializer;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.function.FunctionRegistry;
import edu.ucsd.forward.query.function.FunctionRegistryException;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.visitors.KeyInferenceBuilder;
import edu.ucsd.forward.query.xml.PlanXmlParser;
import edu.ucsd.forward.query.xml.PlanXmlSerializer;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.util.NameGenerator;
import edu.ucsd.forward.util.NameGeneratorFactory;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Tests the key inference builder.
 * 
 * @author Yupeng Fu
 * 
 */
@Test
public class TestKeyInferenceBuilder extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestKeyInferenceBuilder.class);
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testScanKeyInferrer()
    {
        doTest("TestKeyInferenceBuilder-testNewScanKeyInferrer.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testScanKeyInferrer2()
    {
        doTest("TestKeyInferenceBuilder-testNewScanKeyInferrer2.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testScanKeyInferrer3()
    {
        doTest("TestKeyInferenceBuilder-testNewScanKeyInferrer3.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testScanKeyInferrer4()
    {
        doTest("TestKeyInferenceBuilder-testNewScanKeyInferrer4.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testScanKeyInferrer5()
    {
        doTest("TestKeyInferenceBuilder-testNewScanKeyInferrer5.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testScanKeyInferrer6()
    {
        doTest("TestKeyInferenceBuilder-testNewScanKeyInferrer6.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testProductKeyInferrer()
    {
        doTest("TestKeyInferenceBuilder-testProductKeyInferrer.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testLeftOuterJoinKeyInferrer()
    {
        doTest("TestKeyInferenceBuilder-testLeftOuterJoinKeyInferrer.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testGroupByKeyInferrer()
    {
        doTest("TestKeyInferenceBuilder-testGroupByKeyInferrer.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testGroupByKeyInferrer2()
    {
        doTest("TestKeyInferenceBuilder-testGroupByKeyInferrer2.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testGroupByKeyInferrer3()
    {
        doTest("TestKeyInferenceBuilder-testGroupByKeyInferrer3.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testProjectKeyInferrer()
    {
        doTest("TestKeyInferenceBuilder-testProjectKeyInferrer.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testProjectKeyInferrer2()
    {
        doTest("TestKeyInferenceBuilder-testProjectKeyInferrer2.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testProjectKeyInferrer3()
    {
        doTest("TestKeyInferenceBuilder-testProjectKeyInferrer3.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testProjectKeyInferrer4()
    {
        doTest("TestKeyInferenceBuilder-testProjectKeyInferrer4.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testProjectKeyInferrer5()
    {
        doTest("TestKeyInferenceBuilder-testProjectKeyInferrer5.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testProjectKeyInferrer6()
    {
        doTest("TestKeyInferenceBuilder-testProjectKeyInferrer6.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testProjectKeyInferrer7()
    {
        // A group by on top of project
        doTest("TestKeyInferenceBuilder-testProjectKeyInferrer7.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testApplyPlanKeyInferrer()
    {
        doTest("TestKeyInferenceBuilder-testApplyPlanKeyInferrer.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testApplyPlanKeyInferrer2()
    {
        doTest("TestKeyInferenceBuilder-testApplyPlanKeyInferrer2.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testExistsKeyInferrer()
    {
        doTest("TestKeyInferenceBuilder-testExistsKeyInferrer.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testEliminateDuplicates()
    {
        doTest("TestKeyInferenceBuilder-testEliminateDuplicates.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testPartition()
    {
        doTest("TestKeyInferenceBuilder-testPartition.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testPartition2()
    {
        // The key is included in partition-by attributes.
        doTest("TestKeyInferenceBuilder-testPartition2.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testScanKeyInferrer8() throws FunctionRegistryException
    {
        FunctionRegistry registry = FunctionRegistry.getInstance();
        
        /*-
         * FIXME: Use some other function rather than IvmMockFunction
         * if (!registry.hasFunction(IvmMockFunction.NAME)) registry.addApplicationFunction(new IvmMockFunction());
         */
        doTest("TestKeyInferenceBuilder-testNewScanKeyInferrer8.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testUnion()
    {
        // Tests a self-union of a relation with nested collection.
        doTest("TestKeyInferenceBuilder-testUnion.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testUnion2()
    {
        // Tests a union on two bags
        doTest("TestKeyInferenceBuilder-testUnion2.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testUnionAll()
    {
        // Tests a self-union of a relation with nested collection in set.
        doTest("TestKeyInferenceBuilder-testUnionAll.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testUnionAll2()
    {
        // Tests a self-union of a relation with nested collection in bag.
        doTest("TestKeyInferenceBuilder-testUnionAll2.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testOuterUnionAll()
    {
        // Tests a self-union of a relation with nested collection in set.
        doTest("TestKeyInferenceBuilder-testOuterUnionAll.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testOuterUnionAll2()
    {
        // Tests a self-union of a relation with nested collection in bag.
        doTest("TestKeyInferenceBuilder-testOuterUnionAll2.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testOuterUnionAll3()
    {
        // Tests an outer union between different schemas.
        doTest("TestKeyInferenceBuilder-testOuterUnionAll3.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testIntersect()
    {
        doTest("TestKeyInferenceBuilder-testIntersect.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testIntersect2()
    {
        doTest("TestKeyInferenceBuilder-testIntersect2.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testIntersect3()
    {
        // intersect a bag with a set
        doTest("TestKeyInferenceBuilder-testIntersect3.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testIntersectAll()
    {
        // intersect a bag with a set
        doTest("TestKeyInferenceBuilder-testIntersectAll.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testIntersectAll2()
    {
        // intersect a bag with a bag
        doTest("TestKeyInferenceBuilder-testIntersectAll2.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testIntersectAll3()
    {
        // intersect a bag with a bag, but no need for retouching
        doTest("TestKeyInferenceBuilder-testIntersectAll3.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testExcept()
    {
        doTest("TestKeyInferenceBuilder-testExcept.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testExcept2()
    {
        doTest("TestKeyInferenceBuilder-testExcept2.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testExceptAll()
    {
        // except a bag with a set
        doTest("TestKeyInferenceBuilder-testExceptAll.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testExceptAll2()
    {
        // except a bag with a bag
        doTest("TestKeyInferenceBuilder-testExceptAll2.xml");
    }
    
    @Test(groups=NORMAL_FORM_REFACTORING)
    public void testExceptAll3()
    {
        // except a set with a bag
        doTest("TestKeyInferenceBuilder-testExceptAll3.xml");
    }
    
    private void doTest(String plan_xml_file)
    {
        try
        {
            NameGeneratorFactory.getInstance().reset(NameGenerator.KEY_VARIABLE_GENERATOR);
            // Prepare uas
            Element test_case_elm = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), plan_xml_file));
            Element uas_elm = XmlUtil.getOnlyChildElement(test_case_elm, "uas");
            UnifiedApplicationState uas;
            
            uas = new UnifiedApplicationState(DataSourceXmlParser.parse(uas_elm, new LocationImpl(plan_xml_file)));
            
            uas.open();
            
            QueryProcessor qp = QueryProcessorFactory.getInstance();
            qp.setUnifiedApplicationState(uas);
            PlanXmlParser pxp = new PlanXmlParser();
            Element plan_element = XmlUtil.getOnlyChildElement(test_case_elm, "query_plan");
            LogicalPlan logical_plan = pxp.parseLogicalPlan(uas, plan_element);
            
            KeyInferenceBuilder.build(logical_plan);
            
            checkOutputType(QueryProcessorFactory.getInstance().getOutputType(logical_plan, uas), test_case_elm);
            
            // By default, check the retouched query is the same.
            Element retouched_element = XmlUtil.getOptionalChildElement(test_case_elm, "retouched");
            
            LogicalPlan retouched_plan = pxp.parseLogicalPlan(uas, XmlUtil.getOptionalChildElement(retouched_element));
            checkLogicalPlan(logical_plan, retouched_plan, uas);
            
            // Build the plan again, since the key infer algorithm is idempotent
            KeyInferenceBuilder.build(logical_plan);
            checkLogicalPlan(logical_plan, retouched_plan, uas);
            
            uas.close();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Checks if the actual logical plan is the expected one.
     * 
     * @param actual
     *            the actual logical plan.
     * @param expected
     *            the expected logical plan.
     * @param uas
     *            the unified application state.
     */
    protected void checkLogicalPlan(LogicalPlan actual, LogicalPlan expected, UnifiedApplicationState uas)
    {
        PlanXmlSerializer serializer = new PlanXmlSerializer();
        
        Element actual_elm = serializer.serializeLogicalPlan(actual);
        String actual_str = XmlUtil.serializeDomToString(actual_elm, true, true);
        
        Element expected_elm = serializer.serializeLogicalPlan(expected);
        String expected_str = XmlUtil.serializeDomToString(expected_elm, true, true);
        
        assertEquals(expected_str, actual_str);
    }
    
    protected void checkOutputType(Type output_type, Element test_case_elm)
    {
        Element expected_type_element = XmlUtil.getOnlyChildElement(test_case_elm, "schema_tree");
        String expected_str = XmlUtil.serializeDomToString(expected_type_element, true, true);
        
        SchemaTree output_schema = new SchemaTree(output_type);
        String actual_str = TypeXmlSerializer.serializeSchemaTree(output_schema);
        
        assertEquals(expected_str, actual_str);
        
    }
}
