/**
 * 
 */
package edu.ucsd.forward.data.type.extended;

import java.text.ParseException;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;
import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.xml.TypeXmlParser;
import edu.ucsd.forward.xml.XmlParserException;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * @author Hongping Lim
 * 
 */
@Test
public class TestExtendedSchemaTree extends AssertJUnit
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestExtendedSchemaTree.class);
    
    /**
     * Tests that isSchemaTree returns true for a proper schema tree, and that toSchemaTree returns an instance of SchemaTree.
     * 
     * @throws XmlParserException
     *             exception.
     */
    public void testToSchemaTree() throws XmlParserException
    {
        ExtendedSchemaTree tree = getExtendedSchemaTree();
        assertTrue(tree.isSchemaTree());
        SchemaTree schema = tree.toSchemaTree();
        assertTrue(schema.getClass().equals(SchemaTree.class));
    }
    
    public void testIsSchemaTreeTopLevel() throws ParseException, XmlParserException
    {
        testIsSchemaTreeFalse("");
    }
    
    public void testIsSchemaTreeRelation() throws ParseException, XmlParserException
    {
        testIsSchemaTreeFalse("application/tuple");
    }
    
    public void testIsSchemaTreeRelationNested() throws ParseException, XmlParserException
    {
        testIsSchemaTreeFalse("application/tuple/degrees/tuple");
    }
    
    public void testIsSchemaTreeSwitch() throws ParseException, XmlParserException
    {
        testIsSchemaTreeFalse("application/tuple/applying_for/phd");
    }
    
    /**
     * Adds an AnyScalarType at the given path, and tests that the tree return false for the isSchemaTree method.
     * 
     * @param path
     * @throws ParseException
     *             exception.
     * @throws XmlParserException
     *             exception.
     */
    private void testIsSchemaTreeFalse(String path) throws ParseException, XmlParserException
    {
        ExtendedSchemaTree tree = getExtendedSchemaTree();
        // Add an AnyScalarType
        SchemaPath schema_path = new SchemaPath(path);
        TupleType t = (TupleType) schema_path.find(tree);
        t.setAttribute("MyAnyScalarType", new AnyScalarType());
        assertFalse(tree.isSchemaTree());
    }
    
    private ExtendedSchemaTree getExtendedSchemaTree() throws XmlParserException
    {
        String source = "TestExtendedSchemaTree-testIsSchemaTree.xml";
        Element source_elem = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), source));
        SchemaTree schema_tree = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree"));
        
        Type root_type = schema_tree.getRootType();
        // Detach from the tree
        schema_tree.setRootType(null);
        ExtendedSchemaTree tree = new ExtendedSchemaTree(root_type);
        return tree;
        
    }
}
