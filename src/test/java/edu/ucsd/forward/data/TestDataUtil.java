/**
 * 
 */
package edu.ucsd.forward.data;

import java.text.ParseException;

import org.testng.annotations.Test;

import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.xml.TypeXmlParser;
import edu.ucsd.forward.data.xml.ValueXmlParser;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.xml.XmlParserException;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Unit tests for <code>DataUtil</code>.
 * 
 * @author Kian Win
 * 
 */
@Test
public class TestDataUtil extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestDataUtil.class);
    
    /**
     * Tests that cloning creates deep copies.
     * 
     * @throws ParseException
     *             if an error occurs during parsing.
     * @throws XmlParserException
     *             exception.
     */
    public void testCloneDeepCopy() throws ParseException, XmlParserException
    {
        String user_id = "user_id";
        
        String source = "TestDataUtil-testCloneDeepCopy.xml";
        Element source_elem = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), source));
        SchemaTree source_schema = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree"));
        DataTree d1 = ValueXmlParser.parseDataTree(XmlUtil.getOnlyChildElement(source_elem, "data_tree"), source_schema);
        
        // Clone the dataset
        DataTree d2 = ValueUtil.clone(d1);
        
        // Check that a different object is created
        assertTrue(d1 != d2);
        
        // Check that equality still holds
        assertTrue(ValueUtil.deepEquals(d1, d2));
        
        // Check that the cloned object really made a deep copy
        ((TupleValue) new DataPath("/").find(d1)).setAttribute(user_id, new StringValue("sam@green.com"));
        assertTrue(ValueUtil.deepEquals(new StringValue("sam@green.com"), (ScalarValue) new DataPath("/user_id").find(d1)));
        assertTrue(ValueUtil.deepEquals(new StringValue("john@doe.com"), (ScalarValue) new DataPath("/user_id").find(d2)));
    }
    
    /**
     * Tests that cloning resets the immutable flag.
     * 
     * @throws XmlParserException
     *             exception.
     */
    public void testCloneImmutable() throws XmlParserException
    {
        String source = "TestDataUtil-testCloneImmutable.xml";
        Element source_elem = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), source));
        SchemaTree source_schema = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree"));
        DataTree d1 = ValueXmlParser.parseDataTree(XmlUtil.getOnlyChildElement(source_elem, "data_tree"), source_schema);
        
        // Set the immutable flag
        d1.checkTypeConsistent();
        d1.checkConstraintConsistent();
        d1.setDeepImmutable();
        
        DataTree d2 = ValueUtil.clone(d1);
        
        // Check that the immutable flag is not copied
        assertTrue(d1.isDeepImmutable());
        assertFalse(d2.isDeepImmutable());
    }
    
    /**
     * Tests the equality by value method.
     * 
     * @throws XmlParserException
     *             exception.
     */
    public void testEqualByValue() throws XmlParserException
    {
        String source = "TestDataUtil-testEqualByValue-data1.xml";
        Element source_elem = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), source));
        SchemaTree source_schema = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree"));
        DataTree d1 = ValueXmlParser.parseDataTree(XmlUtil.getOnlyChildElement(source_elem, "data_tree"), source_schema);
        
        source = "TestDataUtil-testEqualByValue-data2.xml";
        source_elem = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), source));
        source_schema = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree"));
        DataTree d2 = ValueXmlParser.parseDataTree(XmlUtil.getOnlyChildElement(source_elem, "data_tree"), source_schema);
        
        assertFalse(ValueUtil.deepEquals(d1, d2));
        
        source = "TestDataUtil-testEqualByValue-data3.xml";
        source_elem = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), source));
        source_schema = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree"));
        DataTree d3 = ValueXmlParser.parseDataTree(XmlUtil.getOnlyChildElement(source_elem, "data_tree"), source_schema);
        
        source = "TestDataUtil-testEqualByValue-data4.xml";
        source_elem = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), source));
        source_schema = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree"));
        DataTree d4 = ValueXmlParser.parseDataTree(XmlUtil.getOnlyChildElement(source_elem, "data_tree"), source_schema);
        
        assertFalse(ValueUtil.deepEquals(d3, d4));
    }
}
