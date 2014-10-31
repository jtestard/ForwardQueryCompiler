/**
 * 
 */
package edu.ucsd.forward.fpl;

import static edu.ucsd.forward.xml.XmlUtil.getChildElements;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import com.google.gwt.xml.client.CharacterData;
import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceXmlParser;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.exception.CheckedException;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.util.NameGenerator;
import edu.ucsd.forward.util.NameGeneratorFactory;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * An abstract implementation of the action processor test cases.
 * 
 * @author Yupeng
 * 
 */
@Test
public class AbstractFplTestCase extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger   log             = Logger.getLogger(AbstractFplTestCase.class);
    
    private static final String   ACTION_STMT_ELM = "action_statement";
    
    protected static final String OUTPUT_ELM      = "output";
    
    private static List<String>   s_action_expr   = new ArrayList<String>();
    
    /**
     * Returns all action expressions.
     * 
     * @return a list of action expressions.
     */
    public static List<String> getActionExpressions()
    {
        return s_action_expr;
    }
    
    /**
     * Returns the action expression in the given index.
     * 
     * @param index
     *            the index to retrieve.
     * @return a action expression.
     */
    public static String getActionExpression(int index)
    {
        return s_action_expr.get(index);
    }
    
    /**
     * Parses a test case and sets up the test data sources and data objects to be used by a query processing test case.
     * 
     * @param clazz
     *            the calling class to use as the base path to the test case file.
     * @param relative_file_name
     *            the file name of the test case relative to the calling class.
     * @throws CheckedException
     *             if an error occurs.
     */
    protected static void parseTestCase(Class<?> clazz, String relative_file_name) throws CheckedException
    {
        // Reset the name counter so that the generated numbers are consistent between test runs
        NameGeneratorFactory.getInstance().reset(NameGenerator.VARIABLE_GENERATOR);
        NameGeneratorFactory.getInstance().reset(NameGenerator.PARAMETER_GENERATOR);
        NameGeneratorFactory.getInstance().reset(NameGenerator.PHYSICAL_PLAN_GENERATOR);
        
        s_action_expr.clear();
        
        Element test_case_elm = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(clazz, relative_file_name));
        
        // Close previous unified application state and remove external functions
        // This is necessary because we execute multiple test cases per test method
        if (getUnifiedApplicationState() != null)
        {
            after();
        }
        
        // Build input data sources and schema and data objects
        UnifiedApplicationState uas = new UnifiedApplicationState(DataSourceXmlParser.parse(test_case_elm,
                                                                                            new LocationImpl(relative_file_name)));
        AbstractQueryTestCase.setUnifiedApplicationState(uas);
        
        // Open the unified application state
        getUnifiedApplicationState().open();
        
        // Parse the action expressions
        List<Element> action_expr_elms = getChildElements(test_case_elm, ACTION_STMT_ELM);
        for (Element action_expr_elm : action_expr_elms)
        {
            s_action_expr.add(((CharacterData) action_expr_elm.getChildNodes().item(0)).getData());
        }
        
        parseQueryExpressions(test_case_elm);
    }
}
