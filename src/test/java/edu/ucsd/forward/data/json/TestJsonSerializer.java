/**
 * 
 */
package edu.ucsd.forward.data.json;

import org.testng.annotations.Test;

import com.google.gson.JsonArray;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.XhtmlValue;
import edu.ucsd.forward.query.AbstractQueryTestCase;

/**
 * Tests JSON type and value serializers.
 * 
 * @author Michalis Petropoulos
 * 
 */
@Test
public class TestJsonSerializer extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestJsonSerializer.class);
    
    /**
     * Tests serializing a type.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    public void testTypeJsonSerializer() throws Exception
    {
        parseTestCase(this.getClass(), "TestJsonSerializer-testType.xml");
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        SchemaTree tree = uas.getDataSource("src_1").getSchemaObject("simple").getSchemaTree();
        JsonArray array = TypeJsonSerializer.serializeSchemaTree(tree);
        assertEquals(IoUtil.getResourceAsString("TestJsonSerializer-testTypeSimple.json"), array.toString());
        
        tree = uas.getDataSource("src_1").getSchemaObject("complex").getSchemaTree();
        array = TypeJsonSerializer.serializeSchemaTree(tree);
        assertEquals(IoUtil.getResourceAsString("TestJsonSerializer-testTypeComplex.json"), array.toString());
    }
    
    /**
     * Tests serializing a value.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    public void testValueJsonSerializer() throws Exception
    {
        parseTestCase(this.getClass(), "TestJsonSerializer-testValue.xml");
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        DataTree tree = uas.getDataSource("src_1").getDataObject("simple").getDataTree();
        JsonArray array = ValueJsonSerializer.serializeDataTree(tree);
        assertEquals(IoUtil.getResourceAsString("TestJsonSerializer-testValueSimple.json"), array.toString());
        
        tree = uas.getDataSource("src_1").getDataObject("complex").getDataTree();
        array = ValueJsonSerializer.serializeDataTree(tree, false);
        assertEquals(IoUtil.getResourceAsString("TestJsonSerializer-testValueComplex.json"), array.toString());
    }
    
    /**
     * Tests serializing an xhtml value with entities.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    public void testXhtmlWithEntities() throws Exception
    {
        StringBuilder html_string = new StringBuilder();
        html_string.append("<html><body>");
        html_string.append("&lt; &#60; &gt; &#62;&amp; &#38; &#162; &#163; &#165; &#8364; &#167; &#169; &#174; &#8482; ");
        html_string.append("&cent; &#162; &pound; &#163; &yen; &#165; &euro; &#8364;");
        html_string.append("&sect; &#167; &copy; &#169; &reg;  &#174; &trade; &#8482; &nbsp;");
        html_string.append("</body></html>");
        
        XhtmlValue value = new XhtmlValue(html_string.toString());
        
        DataTree tree = new DataTree(value);
        JsonArray array = ValueJsonSerializer.serializeDataTree(tree);
        assertEquals(IoUtil.getResourceAsString("TestJsonSerializer-testHtmlEntities.json"), array.toString());
    }
    
    /**
     * Tests serializing proper html that is malformed xhtml (no close tag for link element).
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    public void testMalformedXhtml() throws Exception
    {
        StringBuilder html_string = new StringBuilder();
        html_string.append("<html><head>");
        html_string.append("<link href=\"//netdna.bootstrapcdn.com/bootstrap/3.0.3/css/bootstrap.min.css\" rel=\"stylesheet\">");
        html_string.append("</head><body></body></html>");
        
        XhtmlValue value = new XhtmlValue(html_string.toString());
        
        DataTree tree = new DataTree(value);
        JsonArray array = ValueJsonSerializer.serializeDataTree(tree);
        assertEquals(IoUtil.getResourceAsString("TestJsonSerializer-testMalformedXhtml.json"), array.toString());
    }
    
    /**
     * Tests serializing a value with predicates for collection tuples.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    public void testValueJsonSerializerWithPredicates() throws Exception
    {
        parseTestCase(this.getClass(), "TestJsonSerializer-testPredicate.xml");
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        DataTree tree = uas.getDataSource("src_1").getDataObject("complex").getDataTree();
        SchemaTree schema_tree = uas.getDataSource("src_1").getSchemaObject("complex").getSchemaTree();
        tree.setType(schema_tree);
        JsonArray array = ValueJsonSerializer.serializeDataTree(tree);
        assertEquals(IoUtil.getResourceAsString("TestJsonSerializer-testPredicate.json"), array.toString());
    }
}
