/**
 * 
 */
package edu.ucsd.forward.data.json;

import org.testng.annotations.Test;

import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.JsonValue;
import edu.ucsd.forward.data.xml.TypeXmlParser;
import edu.ucsd.forward.data.xml.ValueXmlParser;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Tests the navigation in JSON value.
 * 
 * @author Yupeng
 * 
 */
@Test
public class TestJsonValueNavigation extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestJsonValueNavigation.class);
    
    /**
     * Tests navigate into a JSON value.
     * 
     * @throws Exception
     *             if anything goes wrong.
     */
    public void testTuple() throws Exception
    {
        Element test_case_elm = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString("TestJsonValueNavigation-testTuple.xml"));
        Element schema_element = (Element) test_case_elm.getChildNodes().item(0);
        Element data_element = (Element) test_case_elm.getChildNodes().item(1);
        SchemaTree schema = TypeXmlParser.parseSchemaTree(schema_element);
        DataTree expected_data = ValueXmlParser.parseDataTree(data_element, schema);
        
        JsonValue json_value = (JsonValue) expected_data.getRootValue();
        JsonValue actual = JsonValueNavigationUtil.navigate(json_value, "name");
        
        schema_element = (Element) test_case_elm.getChildNodes().item(2);
        data_element = (Element) test_case_elm.getChildNodes().item(3);
        schema = TypeXmlParser.parseSchemaTree(schema_element);
        expected_data = ValueXmlParser.parseDataTree(data_element, schema);
        AbstractTestCase.assertEqualSerialization(expected_data.getRootValue(), actual);
    }
}
