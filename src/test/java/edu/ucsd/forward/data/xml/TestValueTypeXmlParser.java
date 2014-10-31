/**
 * 
 */
package edu.ucsd.forward.data.xml;

import java.text.ParseException;

import org.testng.annotations.Test;

import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.DataPath;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.xml.XmlParserException;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Unit tests for the data model XML parsers (ValueXmlParser and TypeXmlParser).
 * 
 * @author Kian Win
 * @author Yupeng
 */
@Test
public class TestValueTypeXmlParser extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestValueTypeXmlParser.class);
    
    /**
     * Tests building a data tree, type tree and constraints.
     */
    public void testAll()
    {
        DataTree data_tree = readTestFile("testAll");
        assertNotNull(data_tree);
        assertNotNull(data_tree.getSchemaTree());
        
        // Constructing the expected data structures is tedious. Instead, we eyeball the data structures manually.
    }
    
    /**
     * Tests building a data tree with null tuple and null switch.
     * 
     * @throws ParseException
     *             if an error occurs during parsing.
     */
    public void testNull() throws ParseException
    {
        DataTree data_tree = readTestFile("testNull");
        
        Value value = new DataPath("/applications/tuple[date=2009-12-25]/apply").find(data_tree);
        assertTrue(value instanceof NullValue);
        
        value = new DataPath("/applications/tuple[date=2009-12-26]/apply/applying_for").find(data_tree);
        assertTrue(value instanceof NullValue);
        
        // Expect the find method to return a java null.
        value = new DataPath("/applications/tuple[date=2009-12-25]/apply/applying_for").find(data_tree);
    }
    
    /**
     * Tests building a schema tree with default value.
     * 
     * @throws ParseException
     *             if an error occurs during parsing.
     */
    public void testDefaultValue() throws ParseException
    {
        DataTree data_tree = readTestFile("testDefaultValue");
        
        Value initialized = data_tree.getSchemaTree().getRootType().getDefaultValue();
        
        Value value = new DataPath("/date").find(initialized);
        assertTrue(value == null);
        
        value = new DataPath("/apply/name").find(initialized);
        assertTrue(value == null);
    }
    
    /**
     * Tests parsing the Index.
     * 
     * @throws ParseException
     *             if an error occurs during parsing.
     */
    public void testIndex() throws ParseException
    {
        
        DataTree data_tree = readTestFile("testIndex");
        
        CollectionType type = (CollectionType) data_tree.getSchemaTree().getRootType();
        
        assertEquals(2, type.getIndexDeclarations().size());
        String xml = IoUtil.getResourceAsString(TestValueTypeXmlParser.class, "TestValueTypeXmlParser-" + "testIndex" + ".xml");
        Element node = (Element) XmlUtil.parseDomNode(xml);
        String expected = XmlUtil.serializeDomToString(node, false);
        String actual = TestValueTypeXmlSerializer.serializeDataAndSchema(data_tree);
        assertEquals(expected, actual);
    }
    
    private static DataTree readTestFile(String test_name)
    {
        return readDataAndSchema(TestValueTypeXmlParser.class, "TestValueTypeXmlParser-" + test_name + ".xml");
    }
    
    /**
     * Reads the data tree and schema tree from the input xml file.
     * 
     * @param clazz
     *            resource class
     * @param path
     *            path
     * @return data tree associated with the schema tree
     */
    public static DataTree readDataAndSchema(Class<?> clazz, String path)
    {
        String xml = IoUtil.getResourceAsString(clazz, path);
        Element node = (Element) XmlUtil.parseDomNode(xml);
        Element schema_element = XmlUtil.getOptionalChildElement(node, TypeXmlSerializer.SCHEMA_TREE_ELM);
        Element data_element = XmlUtil.getOptionalChildElement(node, ValueXmlSerializer.DATA_TREE_ELM);
        SchemaTree schema;
        try
        {
            schema = TypeXmlParser.parseSchemaTree(schema_element);
            DataTree data = ValueXmlParser.parseDataTree(data_element, schema);
            data.setType(schema);
            return data;
        }
        catch (XmlParserException e)
        {
            throw new UnsupportedOperationException();
        }
    }
}
