/**
 * 
 */
package edu.ucsd.forward.experiment.set_processable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;
import com.google.gwt.xml.client.NodeList;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceXmlParser;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.CheckedException;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.rewrite.ApplyPlanRemover;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.suspension.SuspensionException;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Abstract class for running set processable experiments.
 * 
 * @author Kian Win Ong
 * 
 */
public abstract class AbstractSetProcessableTestCase extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractSetProcessableTestCase.class);
    
    /**
     * Executes a logical plan over given data objects. The logical plan is compiled over the given UAS, but the given data The
     * given data objects override those specified for compiling in the logical plan.
     * 
     * @param plan_file_name
     *            the file name of the logical plan.
     * @param data_objects_file_name
     *            the file name of the data objects.
     * @param rename_variables_map
     *            a mapping for renaming variables in the logical plan. The mapping is from source variable names to target variable
     *            names. <code>null</code> indicates that no renaming is necessary.
     * @return the execution time.
     * @throws CheckedException
     *             if a checked exception occurs.
     * @throws SuspensionException
     *             if a suspension exception occurs.
     */
    protected static double execute(String plan_file_name, String data_objects_file_name, Map<String, String> rename_variables_map)
            throws CheckedException, SuspensionException
    {
        // Parse the logical plan and data sources, and open the UAS
        parse(plan_file_name, data_objects_file_name, rename_variables_map);
        
        // Obtain the UAS and physical plan
        UnifiedApplicationState uas = getUnifiedApplicationState();
        PhysicalPlan physical_plan = getPhysicalPlan(0);
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        
        // Start timing
        long start = System.currentTimeMillis();
        
        // Execute the physical plan
        Value query_result = QueryProcessorFactory.getInstance().createEagerQueryResult(physical_plan, uas).getValue();
        log.info(query_result.toString());
        // End timing
        long end = (System.currentTimeMillis() - start);
        
        // Clean up
        qp.cleanup(true);
        uas.close();
        qp = null;
        
        return end;
    }
    
    /**
     * Executes a query expression over given data objects. Rewrites the apply-plan if required.
     * 
     * @param query_file_name
     *            the file name of the query expression
     * @param data_objects_file_name
     *            the file name of the data objects.
     * @param rename_variables_map
     *            a mapping for renaming variables in the logical plan. The mapping is from source variable names to target variable
     *            names. <code>null</code> indicates that no renaming is necessary.
     * @param rewrite
     *            whether to rewrite the apply-plan
     * @return the execution time.
     * @throws CheckedException
     *             if a checked exception occurs.
     * @throws SuspensionException
     *             if a suspension exception occurs.
     */
    protected static double executeEndToEnd(String query_file_name, String data_objects_file_name,
            Map<String, String> rename_variables_map, boolean rewrite) throws CheckedException, SuspensionException
    {
        // Parse the logical plan and data sources, and open the UAS
        parse(query_file_name, data_objects_file_name, rename_variables_map);
        
        // Obtain the UAS and physical plan
        UnifiedApplicationState uas = getUnifiedApplicationState();
        String query_string = getQueryExpression(0);
        
        if (rename_variables_map != null)
        {
            for (Map.Entry<String, String> entry : rename_variables_map.entrySet())
            {
                String source = entry.getKey();
                String target = entry.getValue();
                query_string = query_string.replaceAll("public."+source, "public."+target);
            }
        }
        
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        List<AstTree> ast_trees = qp.parseQuery(query_string, new LocationImpl(query_file_name + ".xml"));
        assert (ast_trees.size() == 1);
        
        LogicalPlan actual = qp.translate(Collections.singletonList(ast_trees.get(0)), uas).get(0);
        if (rewrite)
        {
            actual = ApplyPlanRemover.create(true).rewrite(actual);
        }
        LogicalPlan rewritten = qp.rewriteSourceAgnostic(Collections.singletonList(actual), uas).get(0);
        LogicalPlan distributed = qp.distribute(Collections.singletonList(rewritten), uas).get(0);
        PhysicalPlan physical = qp.generate(Collections.singletonList(distributed), uas).get(0);
        
        // Start timing
        long start = System.currentTimeMillis();
        
        // Execute the physical plan
        Value query_result = QueryProcessorFactory.getInstance().createEagerQueryResult(physical, uas).getValue();
        
        log.info(query_result.toString());
        // End timing
        long end = (System.currentTimeMillis() - start);
        
        // Clean up
        qp.cleanup(true);
        uas.close();
        qp = null;
        
        return end;
    }
    
    /**
     * Parses a logical plan and data objects. The given data objects override those in the logical plan.
     * 
     * @param plan_file_name
     *            the file name of the logical plan.
     * @param data_objects_file_name
     *            the file name of the data objects.
     * @param rename_variables_map
     *            a mapping for renaming variables in the logical plan. The mapping is from source variable names to target variable
     *            names. <code>null</code> indicates that no renaming is necessary.
     * @throws CheckedException
     *             if a checked exception occurs.
     */
    protected static void parse(String plan_file_name, String data_objects_file_name, Map<String, String> rename_variables_map)
            throws CheckedException
    {
        // Parse the XML file of the logical plan
        Element plan_element = parseXml(plan_file_name);
        
        // Rename variables in the logical plan (if necessary)
        if (rename_variables_map != null)
        {
            Document document = plan_element.getOwnerDocument();
            NodeList elements = document.getElementsByTagName("Variable");
            for (int i = 0; i < elements.getLength(); i++)
            {
                Element element = (Element) elements.item(i);
                String schema_object = element.getAttribute("schema_object");
                for (Map.Entry<String, String> entry : rename_variables_map.entrySet())
                {
                    String source = entry.getKey();
                    String target = entry.getValue();
                    if (!schema_object.equals(source)) continue;
                    element.setAttribute("schema_object", target);
                }
            }
            
        }
        
        // Parse the test case from the XML DOM
        parseTestCase(plan_element, plan_file_name);
        
        if (data_objects_file_name != null)
        {
            // Close the UAS before overriding data sources
            UnifiedApplicationState uas = getUnifiedApplicationState();
            uas.close();
            
            // Parse the XML file of the data sources
            Element selected_nations_element = parseXml(data_objects_file_name);
            
            // Add data sources to the UAS (override the data source of the same name in the logical plan)
            for (DataSource data_source : DataSourceXmlParser.parse(selected_nations_element,
                                                                    new LocationImpl(data_objects_file_name)))
            {
                String name = data_source.getMetaData().getName();
                uas.removeDataSource(name);
                uas.addDataSource(data_source);
            }
            
            uas.open();
        }
    }
    
    /**
     * Parse a XML file.
     * 
     * @param file_name
     *            the file name.
     * @return the root element.
     */
    private static Element parseXml(String file_name)
    {
        return (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(AbstractSetProcessableTestCase.class, file_name));
    }
    
}
