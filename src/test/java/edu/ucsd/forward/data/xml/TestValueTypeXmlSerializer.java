/**
 * 
 */
package edu.ucsd.forward.data.xml;

import org.testng.annotations.Test;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Tests data model XML serializers (ValueXmlSerializer and TypeXmlSerializer).
 * 
 * @author Yupeng
 * 
 */
@Test
public class TestValueTypeXmlSerializer extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestValueTypeXmlSerializer.class);
    
    /**
     * Tests converting a flat data tree into XML representation.
     */
    public void testFlat()
    {
        verify("testFlat");
    }
    
    /**
     * Tests converting a nested data tree into XML representation.
     */
    public void testNestedRelation()
    {
        verify("testNestedRelation");
    }
    
    private void verify(String test_name)
    {
        String test_file = getTestFile(test_name);
        DataTree data = readTestFile(test_file);
        
        String output = IoUtil.normalizeLineBreak(serializeDataAndSchema(data).trim());
        String expected = IoUtil.normalizeLineBreak(IoUtil.getResourceAsString(test_file).trim());
        
        assertEquals(expected, output);
    }
    
    private static DataTree readTestFile(String test_file)
    {
        return TestValueTypeXmlParser.readDataAndSchema(TestValueTypeXmlSerializer.class, test_file);
    }
    
    private static String getTestFile(String test_name)
    {
        return "TestValueTypeXmlSerializer-" + test_name + ".xml";
    }
    
    /**
     * Serializes the data tree and its schema tree into xml under a "test_case" root element.
     * 
     * @param data_tree
     *            data tree
     * @return xml string
     */
    public static String serializeDataAndSchema(DataTree data_tree)
    {
        Document d = XmlUtil.createDocument();
        Element node = d.createElement("test_case");
        d.appendChild(node);
        TypeXmlSerializer.serializeSchemaTree(data_tree.getSchemaTree(), node);
        ValueXmlSerializer.serializeDataTree(data_tree, node);
        return XmlUtil.serializeDomToString(d, false);
    }
}
