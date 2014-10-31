/**
 * 
 */
package edu.ucsd.forward.data.type;

import org.testng.annotations.Test;

import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.xml.TypeXmlParser;
import edu.ucsd.forward.data.xml.TypeXmlSerializer;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.xml.XmlParserException;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Unit tests for <code>TypeUtil</code>.
 * 
 * @author Kian Win
 * @author Kevin Zhao
 */
@Test
public class TestTypeUtil extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestTypeUtil.class);
    
    /**
     * Tests cloning a type sub-tree without its ancestors.
     */
    public void testCloneNoParent()
    {
        CollectionType r1 = new CollectionType();
        TupleType t1 = new TupleType();
        StringType s1 = new StringType();
        t1.setAttribute("string", s1);
        r1.setTupleType(t1);
        
        TupleType t2 = TypeUtil.cloneNoParent(t1);
        StringType s2 = (StringType) t2.getAttribute("string");
        
        assertNull(t2.getParent());
        assertNotSame(t1, t2);
        assertNotSame(s1, s2);
    }
    
    public void testEqualsByHomomorphismExceptSwitchCases() throws XmlParserException
    {
        String source = "TestTypeUtil-testEqualsByHomomorphismExceptSwitchCases(a).xml";
        Element source_elem = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), source));
        SchemaTree a = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree"));
        
        source = "TestTypeUtil-testEqualsByHomomorphismExceptSwitchCases(b).xml";
        source_elem = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), source));
        SchemaTree b = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree"));
        
        assertFalse(TypeUtil.deepEqualsByHomomorphism(a, b));
        assertTrue(TypeUtil.deepEqualsByHomomorphismExceptSwitchCases(a, b));
        assertTrue(TypeUtil.deepEqualsByIsomorphismWithRightSwitchCasesUnmapped(b, a));
    }
    
    /**
     * Tests cloning of a schema and its constraints.
     * 
     * @throws XmlParserException
     *             exception.
     */
    public void testCloneSchema() throws XmlParserException
    {
        String source = "TestTypeUtil-testCloneSchemaAndConstraints.xml";

        Element source_elem = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), source));
        SchemaTree a = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree"));
        
        SchemaTree b = TypeUtil.clone(a);
        
        // assertTrue(b.getConstraints().size() > 0);
        // assertEquals(a.getConstraints().size(), b.getConstraints().size());
        // for (int i = 0; i < a.getConstraints().size(); i++)
        // {
        // Constraint a_constraint = a.getConstraints().get(i);
        // Constraint b_constraint = a.getConstraints().get(i);
        //
        // assertTrue(b_constraint.getAppliedTypes().size() > 0);
        // assertEquals(a_constraint.getAppliedTypes().size(), b_constraint.getAppliedTypes().size());
        //
        // for (int j = 0; j < a_constraint.getAppliedTypes().size(); j++)
        // {
        // Type a_schema = a_constraint.getAppliedTypes().get(j);
        // Type b_schema = b_constraint.getAppliedTypes().get(j);
        // assertEquals(new SchemaPath(a_schema).getString(), new SchemaPath(b_schema).getString());
        // }
        // }
        assertTrue(TypeUtil.deepEqualsByIsomorphism(a, b));
    }
    
    /**
     * Tests deep union.
     * 
     * @throws XmlParserException
     *             exception
     */
    public void testDeepUnion() throws XmlParserException
    {
        Element source_elem = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(),
                                                                                        "TestTypeUtil-testDeepUnion.xml"));
        SchemaTree a = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree1"));
        
        SchemaTree b = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree2"));
        
        SchemaTree c = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree3"));
        
        Type root_type = TypeUtil.deepUnion(a.getRootType(), b.getRootType());
        assertEquals(TypeXmlSerializer.serializeSchemaTree(c), TypeXmlSerializer.serializeSchemaTree(new SchemaTree(root_type)));
    }
}
