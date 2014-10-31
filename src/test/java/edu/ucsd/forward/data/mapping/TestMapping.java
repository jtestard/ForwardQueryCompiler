/**
 * 
 */
package edu.ucsd.forward.data.mapping;

import java.util.Collections;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.SchemaObject;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * Tests updating mapped data.
 * 
 * @author Yupeng Fu
 * 
 */
@Test
public class TestMapping extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestMapping.class);
    
    /**
     * Tests insert.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testInsert() throws Exception
    {
        verify("TestMapping-testInsert", true);
    }
    
    /**
     * Tests insert into nested collection.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testInsertNested() throws Exception
    {
        verify("TestMapping-testInsertNested1", true);
        verify("TestMapping-testInsertNested2", true);
    }
    
    /**
     * Tests delete.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testDelete() throws Exception
    {
        verify("TestMapping-testDelete", true);
    }
    
    /**
     * Tests delete from nested collection.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testDeleteNested() throws Exception
    {
        verify("TestMapping-testDeleteNested1", true);
        verify("TestMapping-testDeleteNested2", true);
        verify("TestMapping-testDeleteNested3", true);
        verify("TestMapping-testDeleteNested4", true);
    }
    
    /**
     * Tests update.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testUpdate() throws Exception
    {
        verify("TestMapping-testUpdate", true);
    }
    
    /**
     * Tests update.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testUpdateNested() throws Exception
    {
        verify("TestMapping-testUpdateNested1", true);
        verify("TestMapping-testUpdateNested2", true);
    }
    
    /**
     * Verifies the output of the query processor.
     * 
     * @param relative_file_name
     *            the relative path of the XML file specifying the test case.
     * @param check_output
     *            whether to check the output of the query.
     * @throws Exception
     *             if an error occurs.
     */
    protected void verify(String relative_file_name, boolean check_output) throws Exception
    {
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), relative_file_name + ".xml");
        
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        // Move the context object into the fpl_local_scope
        uas.requestNewFplSource();
        DataTree context = uas.getDataSource(DataSource.CONTEXT).getDataObject(DataSource.CONTEXT);
        SchemaObject context_schema = uas.getDataSource(DataSource.CONTEXT).getSchemaObject(DataSource.CONTEXT);
        uas.getDataSource(DataSource.FPL_LOCAL_SCOPE).createSchemaObject(DataSource.CONTEXT, context_schema.getSchemaTree(),
                                                                         context_schema.getCardinalityEstimate());
        uas.getDataSource(DataSource.FPL_LOCAL_SCOPE).setDataObject(DataSource.CONTEXT, context);
        
        // Instantiate the target path
        DataTree event_state = uas.getDataSource(DataSource.PAGE).getDataObject("event");
        uas.getMappingGroups().get(0).instantiate(event_state);
        
        // Get the DMl query
        for (String query_expr : getQueryExpressions())
        {
            AstTree ast_tree = qp.parseQuery(query_expr, new LocationImpl(relative_file_name + ".xml")).get(0);
            
            PhysicalPlan physical_plan = qp.compile(Collections.singletonList(ast_tree), uas).get(0);
            // Execute the plan
            QueryProcessorFactory.getInstance().createEagerQueryResult(physical_plan, uas).getValue();
        }
        
        // Check the modification on the mapped data
        DataTree page_after = uas.getDataSource("__page").getDataObject("page");
        DataTree expected_page_after = uas.getDataSource(OUTPUT_ELM).getDataObject("page");
        
        AbstractTestCase.assertEqualSerialization(expected_page_after.getRootValue(), page_after.getRootValue());
        uas.removeTopFplSource();
    }
}
