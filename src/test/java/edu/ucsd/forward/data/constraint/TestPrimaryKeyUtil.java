/**
 * 
 */
package edu.ucsd.forward.data.constraint;

import java.text.ParseException;
import java.util.List;

import org.testng.annotations.Test;

import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.DataPath;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.data.xml.TypeXmlParser;
import edu.ucsd.forward.data.xml.ValueXmlParser;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.xml.XmlParserException;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Unit tests for <code>PrimaryKeyUtil</code>.
 * 
 * @author Kian Win
 * 
 */
@Test
public class TestPrimaryKeyUtil extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestPrimaryKeyUtil.class);
    
    /**
     * Tests getting the global primary key when there is no explicitly specified global primary key constraint.
     * 
     * @throws XmlParserException
     *             exception.
     */
    public void testNoGlobalPrimaryKeyConstraint() throws ParseException, XmlParserException
    {
        String source = "TestPrimaryKeyUtil-testNoGlobalPrimaryKeyConstraint.xml";
        Element source_elem = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), source));
        SchemaTree schema_tree = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree"));
        
        {
            // Global primary key attributes for a top relation type
            CollectionType relation_type = (CollectionType) new SchemaPath("application").find(schema_tree);
            List<ScalarType> attributes = PrimaryKeyUtil.getGlobalPrimaryKeyTypes(relation_type);
            assertSame(1, attributes.size());
            assertSame(new SchemaPath("application/tuple/application_id").find(schema_tree), attributes.get(0));
        }
        
        {
            // Global primary key attributes for a nested relation type
            CollectionType relation_type = (CollectionType) new SchemaPath("application/tuple/degrees").find(schema_tree);
            List<ScalarType> attributes = PrimaryKeyUtil.getGlobalPrimaryKeyTypes(relation_type);
            assertSame(2, attributes.size());
            assertSame(new SchemaPath("application/tuple/application_id").find(schema_tree), attributes.get(0));
            assertSame(new SchemaPath("application/tuple/degrees/tuple/degree_id").find(schema_tree), attributes.get(1));
        }
    }
    
    public void testGetGlobalPrimaryKeyValues() throws ParseException, XmlParserException
    {
        String source = "TestPrimaryKeyUtil-testGetGlobalPrimaryKeyValues.xml";
        Element source_elem = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), source));
        SchemaTree schema_tree = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree"));
        DataTree data_tree = ValueXmlParser.parseDataTree(XmlUtil.getOnlyChildElement(source_elem, "data_tree"), schema_tree);
        
        DataPath path = new DataPath("/applications/tuple[application_id=123,application_id2=456]/degrees/tuple[degree_id=3]");
        
        Value data = path.find(data_tree);
        assertTrue(data instanceof TupleValue);
        
        List<ScalarValue> primary_keys = PrimaryKeyUtil.getGlobalPrimaryKeyValues((TupleValue) data);
        
        assertTrue(primary_keys.size() == 3);
        
        DataPath key1_path = new DataPath("/applications/tuple[application_id=123,application_id2=456]/application_id");
        DataPath key2_path = new DataPath("/applications/tuple[application_id=123,application_id2=456]/application_id2");
        DataPath key3_path = new DataPath(
                                          "/applications/tuple[application_id=123,application_id2=456]/degrees/tuple[degree_id=3]/degree_id");
        
        Value key1 = key1_path.find(data_tree);
        Value key2 = key2_path.find(data_tree);
        Value key3 = key3_path.find(data_tree);
        
        assertTrue(primary_keys.get(0) == key1);
        assertTrue(primary_keys.get(1) == key2);
        assertTrue(primary_keys.get(2) == key3);
    }
}
